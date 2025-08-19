import os
import sys
import poplib
import email
import pathlib
import uuid
from email.header import decode_header, make_header
from dotenv import load_dotenv
import pyodbc
import datetime
import json
import re
from email.utils import parseaddr

# -------------------- Setup for PyInstaller --------------------
def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS  # type: ignore[attr-defined]
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# -------------------- Load Config --------------------
dotenv_path = resource_path(".env")
load_dotenv(dotenv_path)

def load_creds(file_path=resource_path("creds.txt")):
    if not os.path.exists(file_path):
        print(f"[ERROR] Missing credentials file: {file_path}")
        return
    with open(file_path, "r", encoding="utf-8") as fh:
        for line in fh:
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.strip().split("=", 1)
                os.environ[k.strip()] = v.strip()

load_creds()

# -------------------- Directories --------------------
ATTACH_DIR = pathlib.Path(os.getenv("ATTACH_DIR", "attachments"))
ATTACH_DIR.mkdir(parents=True, exist_ok=True)

# -------------------- Database Connection --------------------
def db_connect():
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('DB_HOST')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASS')}"
    )
    return pyodbc.connect(connection_string, autocommit=False)

# -------------------- Small helpers --------------------
def fit(s: str | None, n: int) -> str:
    return (s or "")[:n]

def decode_mime_words(s: str) -> str:
    return str(make_header(decode_header(s))) if s else ""

def is_undelivered(msg) -> bool:
    needles = ("undelivered", "return to sender", "mail delivery failed",
               "delivery status notification", "mailer-daemon", "bounce")
    subj = decode_mime_words(msg.get("Subject", "")).lower()
    if any(n in subj for n in needles):
        return True
    for part in msg.walk():
        ctype = part.get_content_type()
        if ctype in ("text/plain", "text/html"):
            try:
                txt = part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8", "ignore").lower()
            except Exception:
                continue
            if any(n in txt for n in needles):
                return True
    return False

# -------------------- Member Table Functions --------------------
def fetch_member(cur, ffnum):
    cur.execute("""
        SELECT TITLE, FNAME, LNAME, tier
        FROM dbo.member
        WHERE FFNUM = ?
    """, (ffnum,))
    row = cur.fetchone()
    if not row:
        return None
    return {"title": row.TITLE, "fname": row.FNAME, "lname": row.LNAME, "tier": row.tier}

def store_member(cur, ffnum, emailaddr, title=None, fname=None, lname=None, tier=None):
    cur.execute("SELECT 1 FROM dbo.member WHERE FFNUM = ?", (ffnum,))
    if cur.fetchone():
        return
    cur.execute("""
        INSERT INTO dbo.member (FFNUM, EMAIL, TITLE, FNAME, LNAME, tier)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (ffnum, fit(emailaddr, 200), fit(title, 50), fit(fname, 50), fit(lname, 50), fit(tier, 1)))
    print(f"[+] Created member record: FFNUM={ffnum}")

def fetch_member_by_email(cur, emailaddr):
    cur.execute("""
        SELECT FFNUM, TITLE, FNAME, LNAME, tier
        FROM dbo.member
        WHERE EMAIL = ?
    """, (emailaddr,))
    row = cur.fetchone()
    if not row:
        return None
    return {"ffnum": row.FFNUM, "title": row.TITLE, "fname": row.FNAME, "lname": row.LNAME, "tier": row.tier}

# -------------------- Ticket number (no identity; safe MAX+1) --------------------
def next_ticket_number(cur) -> int:
    # SERIALIZABLE + UPDLOCK/HOLDLOCK to avoid duplicates across concurrent workers
    cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
    cur.execute("SELECT ISNULL(MAX(tkt_no), 0) FROM dbo.cms WITH (UPDLOCK, HOLDLOCK)")
    return int((cur.fetchone()[0] or 0) + 1)

# -------------------- UUID log --------------------
def log_uuid(ticket_no, uid):
    logf = "ticket_uuids.json"
    data = {}
    if os.path.exists(logf):
        try:
            with open(logf, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            data = {}
    data[str(ticket_no)] = uid
    with open(logf, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    print(f"[+] UUID {uid} logged for ticket {ticket_no}")

# -------------------- File saving --------------------
def sanitize_filename(name: str, ticket_no: int) -> str:
    name = f"{ticket_no}_{name}"
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > 150:
        root, ext = os.path.splitext(name)
        name = root[:140] + ext[:10]
    return name

def save_attachment(part, uid, ticket_no):
    fn = decode_mime_words(part.get_filename() or "")
    if not fn:
        return None
    safe = sanitize_filename(fn, ticket_no)
    fld = ATTACH_DIR / str(ticket_no)
    fld.mkdir(parents=True, exist_ok=True)
    path = fld / safe
    with open(path, "wb") as fh:
        fh.write(part.get_payload(decode=True))
    print(f"[+] Attachment saved: {path}")
    return str(path)

def extract_and_save_body(msg, ticket_number, cur, ffnum):
    body_content, is_html = None, False
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            ctype = part.get_content_type()
            if ctype == "text/html":
                try:
                    body_content = part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="ignore"
                    )
                    is_html = True
                    break
                except Exception:
                    body_content = None
            elif ctype == "text/plain" and not is_html:
                try:
                    body_content = part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="ignore"
                    )
                except Exception:
                    body_content = None
    else:
        try:
            body_content = msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8", errors="ignore"
            )
        except Exception:
            body_content = None

    if not body_content:
        body_content = "(No message content)"
    if not is_html:
        body_content = f"<html><body><pre>{body_content}</pre></body></html>"

    member = fetch_member(cur, ffnum) if ffnum else None
    if member:
        membership_block = (
            "<hr><div><strong>Membership Information</strong></div><ul>"
            f"<li><strong>FFNUM:</strong> {ffnum}</li>"
            f"<li><strong>Title:</strong> {member['title']}</li>"
            f"<li><strong>First Name:</strong> {member['fname']}</li>"
            f"<li><strong>Last Name:</strong> {member['lname']}</li>"
            f"<li><strong>Tier:</strong> {member['tier']}</li>"
            "</ul>"
        )
    else:
        membership_block = (
            "<hr><div><strong>Membership Information</strong></div>"
            f"<p>No record found for FFNUM <em>{ffnum or 'N/A'}</em>.</p>"
        )

    if re.search(r"</body\s*>", body_content, flags=re.IGNORECASE):
        body_content = re.sub(r"</body\s*>", membership_block + "</body>", body_content, flags=re.IGNORECASE)
    else:
        body_content += membership_block

    path = ATTACH_DIR / f"{ticket_number}.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body_content)
    print(f"[+] Email body saved with membership info: {path}")
    return str(path)

# -------------------- CMS Inserts --------------------
def store_undelivered_email(cur, sender, reason):
    cur.execute(
        "INSERT INTO undelivered_emails(sender_email,date_received,reason) VALUES(?,?,?)",
        (fit(sender, 200), datetime.datetime.now(), fit(reason, 200)),
    )

def store_in_cms(cur, **vals):
    cur.execute(
        """
        INSERT INTO dbo.cms(
          tkt_no,
          ffnum, Req_Date, Category, Subject, cstatus, UpdateDate, UpdateBy,
          fwd_to, fwd_date, fwd_remarks, fwd_by, attachments, email,
          TopCategory, CorporateDetails, urgent, Req_By, PointsExp, tier,
          download_datetime, hitit_ref_no
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            vals['tkt_no'],
            fit(vals['ffnum'], 250), vals['Req_Date'], fit(vals['Category'], 50), fit(vals['Subject'], 1600),
            fit(vals['cstatus'], 1), vals['UpdateDate'], fit(vals['UpdateBy'], 50), fit(vals['fwd_to'], 50),
            vals['fwd_date'], fit(vals['fwd_remarks'], 250), fit(vals['fwd_by'], 50), fit(vals['attachments'], 100),
            fit(vals['email'], 200), fit(vals['TopCategory'], 30), fit(vals['CorporateDetails'], 12),
            fit(vals['urgent'], 5), fit(vals['Req_By'], 25), fit(vals['PointsExp'], 1), fit(vals['tier'], 1),
            vals['download_datetime'], fit(vals['hitit_ref_no'], 50)
        )
    )

# -------------------- POP3 helpers --------------------
def get_uidl_map(pop_conn):
    try:
        resp, listings, _ = pop_conn.uidl()
    except TypeError:
        resp, listings = pop_conn.uidl()
    idx_to_uid = {}
    for line in listings:
        if isinstance(line, bytes): line = line.decode("utf-8", "ignore")
        parts = line.strip().split()
        if len(parts) >= 2 and parts[0].isdigit():
            idx_to_uid[int(parts[0])] = parts[1]
    return idx_to_uid

def load_seen_uidls(path="seen_uidls.json"):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return set(json.load(fh))
        except Exception:
            return set()
    return set()

def save_seen_uidls(seen, path="seen_uidls.json"):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(sorted(seen), fh, indent=2)

# -------------------- Email Processing --------------------
def process_email(msg, cur, db):
    raw_sender = decode_mime_words(msg.get("From", ""))
    _, sender = parseaddr(raw_sender)
    subj = decode_mime_words(msg.get("Subject", ""))

    if is_undelivered(msg):
        store_undelivered_email(cur, sender, "Undelivered Mail/Return")
        print(f"[!] Undelivered: {subj}")
        return True, None

    member_info = fetch_member_by_email(cur, sender)
    ff = member_info['ffnum'] if member_info else None
    tier = fit(member_info['tier'], 1) if member_info else ""

    # Start SERIALIZABLE window to compute next ticket safely
    cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
    try:
        tno = next_ticket_number(cur)  # locked MAX+1
        # Save body now that we know tkt_no; keep DB path short (<=100)
        body_path_fs = extract_and_save_body(msg, tno, cur, ff)  # returns filesystem path
        body_path_db = fit(f"{ATTACH_DIR.name}/{tno}.html", 100)

        store_in_cms(
            cur,
            tkt_no            = tno,
            ffnum             = ff,
            Req_Date          = datetime.date.today(),
            Category          = "General",
            Subject           = subj,
            cstatus           = "N",
            UpdateDate        = datetime.date.today(),
            UpdateBy          = "",             # <=50
            fwd_to            = None,
            fwd_date          = None,
            fwd_remarks       = None,
            fwd_by            = None,
            attachments       = body_path_db,   # <=100
            email             = sender,
            TopCategory       = "",
            CorporateDetails  = None,
            urgent            = "No",
            Req_By            = fit(sender.split("@")[0], 25),
            PointsExp         = "",
            tier              = tier,
            download_datetime = datetime.datetime.now(),
            hitit_ref_no      = None,
        )

        db.commit()
    except Exception as e:
        db.rollback()
        raise

    # Save file attachments (best-effort, outside the serializable window)
    for part in msg.walk():
        if part.get_filename():
            try:
                save_attachment(part, str(uuid.uuid4()), tno)
            except Exception as e:
                print(f"[WARN] Failed to save attachment for ticket {tno}: {e}")

    log_uuid(tno, str(uuid.uuid4()))
    print(f"[✓] Ticket {tno} complete for {raw_sender}")
    return True, tno

# -------------------- Entry Point --------------------
def main():
    try:
        pop_conn = poplib.POP3_SSL(os.getenv("POP3_HOST"), int(os.getenv("POP3_PORT")))
        pop_conn.user(os.getenv("POP3_USER"))
        pop_conn.pass_(os.getenv("POP3_PASS"))
        print(pop_conn.getwelcome().decode(errors="ignore"))
    except Exception as e:
        print(f"[ERROR] POP3 connect: {e}")
        return

    try:
        db = db_connect()
        cur = db.cursor()
    except Exception as e:
        print(f"[ERROR] DB connect: {e}")
        try: pop_conn.quit()
        except Exception: pass
        return

    # UIDL map + seen cache
    try:
        idx_to_uid = get_uidl_map(pop_conn)
    except Exception as e:
        print(f"[ERROR] POP3 UIDL: {e}")
        idx_to_uid = {}
    seen = load_seen_uidls()

    try:
        msg_count = len(pop_conn.list()[1])
        print(f"[i] Messages on server: {msg_count}")

        for i in range(1, msg_count + 1):
            uid = idx_to_uid.get(i)
            if uid and uid in seen:
                continue
            try:
                raw_lines = pop_conn.retr(i)[1]
                raw = b"\n".join(raw_lines)
                msg = email.message_from_bytes(raw)

                ok, tno = process_email(msg, cur, db)

                if ok:
                    try:
                        pop_conn.dele(i)
                        if uid: seen.add(uid)
                    except Exception as de:
                        print(f"[WARN] Could not delete message {i}: {de}")

            except Exception as e:
                print(f"[ERROR] Processing message {i}: {e}")
                # rollback already done inside process_email if needed

        save_seen_uidls(seen)

    except Exception as e:
        print(f("[ERROR] Processing loop: {e}"))

    try:
        cur.close(); db.close()
    except Exception:
        pass
    try:
        pop_conn.quit()
    except Exception:
        pass

    print("[✓] All done.")

    # --------------------  Bulk Delete  --------------------
# NOTE: This function is commented out by default for safety.
# To use:
#   1) Remove the leading '#' from the function and example call below.
#   2) Adjust the criteria as needed.
#   3) Remember: POP3 deletions are only finalized after pop_conn.quit().
#
# import time
# from datetime import timezone
#
# def delete_emails(pop_conn,
#                   before_date: datetime.datetime | None = None,
#                   from_contains: str | None = None,
#                   subject_regex: str | None = None,
#                   undelivered_only: bool | None = None,
#                   dry_run: bool = True) -> dict:
#     """
#     Bulk-delete emails on the POP3 server that match the given criteria.
#
#     Parameters
#     ----------
#     pop_conn : poplib.POP3 or poplib.POP3_SSL
#         An authenticated POP3 connection.
#     before_date : datetime.datetime | None
#         Delete only messages with Date header strictly before this UTC datetime.
#         If naive, will be treated as UTC.
#     from_contains : str | None
#         Case-insensitive substring that must appear in the sender email address.
#     subject_regex : str | None
#         Regex pattern (case-insensitive) that must match the Subject.
#     undelivered_only : bool | None
#         If True, only delete messages detected as undelivered/bounce (via is_undelivered()).
#         If False, only delete messages that are NOT undelivered.
#         If None, ignore this criterion.
#     dry_run : bool
#         If True, do not actually mark messages for deletion; just report what would happen.
#
#     Returns
#     -------
#     dict with keys:
#       'checked': int      # number of messages examined
#       'matched': int      # number of messages matching the filters
#       'deleted': int      # number of messages marked for deletion (0 in dry_run)
#       'errors': list[str] # any per-message errors
#     """
#     results = {'checked': 0, 'matched': 0, 'deleted': 0, 'errors': []}
#     # Normalize before_date to aware UTC if provided
#     if before_date and before_date.tzinfo is None:
#         before_date = before_date.replace(tzinfo=timezone.utc)
#
#     try:
#         # Build UIDL map for possible audit/logging
#         try:
#             _, listings = pop_conn.uidl()
#         except TypeError:
#             _, listings, _ = pop_conn.uidl()
#         uidl_by_index = {}
#         for line in listings:
#             if isinstance(line, bytes): line = line.decode("utf-8", "ignore")
#             parts = line.strip().split()
#             if len(parts) >= 2 and parts[0].isdigit():
#                 uidl_by_index[int(parts[0])] = parts[1]
#
#         # Count messages
#         msg_count = len(pop_conn.list()[1])
#
#         # Pre-compile subject regex
#         subj_rx = re.compile(subject_regex, flags=re.IGNORECASE) if subject_regex else None
#
#         for i in range(1, msg_count + 1):
#             results['checked'] += 1
#             uid = uidl_by_index.get(i, "?")
#             try:
#                 # Try header-only fetch to avoid downloading large bodies
#                 headers_lines = None
#                 try:
#                     resp, headers_lines, _ = pop_conn.top(i, 0)  # Some servers may not support TOP
#                 except Exception:
#                     headers_lines = None
#
#                 if headers_lines:
#                     raw = b"\n".join(headers_lines)
#                     msg = email.message_from_bytes(raw)
#                 else:
#                     # Fallback to full retrieval
#                     raw_lines = pop_conn.retr(i)[1]
#                     raw = b"\n".join(raw_lines)
#                     msg = email.message_from_bytes(raw)
#
#                 raw_sender = decode_mime_words(msg.get("From", ""))
#                 _, sender_addr = parseaddr(raw_sender)
#                 subj = decode_mime_words(msg.get("Subject", ""))
#
#                 # Date check (parse "Date" header; if absent or unparsable, we skip the before_date test)
#                 msg_date_ok = True
#                 if before_date:
#                     hdr_date = msg.get("Date")
#                     if hdr_date:
#                         try:
#                             # email.utils.parsedate_to_datetime available in Py3.3+
#                             msg_dt = email.utils.parsedate_to_datetime(hdr_date)
#                             if msg_dt.tzinfo is None:
#                                 msg_dt = msg_dt.replace(tzinfo=timezone.utc)
#                             if not (msg_dt < before_date):
#                                 msg_date_ok = False
#                         except Exception:
#                             # If we cannot parse date, treat as not matching the before_date criterion
#                             msg_date_ok = False
#                     else:
#                         msg_date_ok = False
#
#                 # Sender filter
#                 sender_ok = True
#                 if from_contains:
#                     sender_ok = (from_contains.lower() in (sender_addr or "").lower())
#
#                 # Subject regex
#                 subject_ok = True
#                 if subj_rx:
#                     subject_ok = bool(subj_rx.search(subj or ""))
#
#                 # Undelivered filter (requires body in worst case; we’ll try headers first and fall back)
#                 undelivered_ok = True
#                 if undelivered_only is not None:
#                     # Quick header check is insufficient; ensure we detect with body when needed
#                     checked_msg = msg
#                     # If TOP ran, msg may lack body; do full RETR just for undelivered check
#                     if headers_lines and undelivered_only is not None:
#                         try:
#                             raw_lines_full = pop_conn.retr(i)[1]
#                             raw_full = b"\n".join(raw_lines_full)
#                             checked_msg = email.message_from_bytes(raw_full)
#                         except Exception:
#                             pass
#                     bounce = is_undelivered(checked_msg)
#                     undelivered_ok = (bounce is True) if undelivered_only else (bounce is False)
#
#                 # Combine criteria
#                 matches = msg_date_ok and sender_ok and subject_ok and undelivered_ok
#                 if matches:
#                     results['matched'] += 1
#                     # Mark for deletion if not a dry run
#                     if not dry_run:
#                         pop_conn.dele(i)
#                         results['deleted'] += 1
#
#             except Exception as msg_err:
#                 results['errors'].append(f"Index {i} UIDL {uid}: {msg_err}")
#
#         return results
#
#     except Exception as e:
#         results['errors'].append(str(e))
#         return results
#
# # -------------------- Example usage (commented) --------------------
# # Inside main(), AFTER successful POP3 login and BEFORE pop_conn.quit():
# #
# # try:
# #     # Example: delete all undelivered/bounce emails older than 14 days
# #     cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=14)
# #     report = delete_emails(
# #         pop_conn,
# #         before_date=cutoff,
# #         from_contains=None,
# #         subject_regex=r"(delivery status notification|undelivered|mailer-daemon)",
# #         undelivered_only=True,
# #         dry_run=True  # ← set False to actually delete
# #     )
# #     print(f"[i] DELETE REPORT: {report}")
# # except Exception as e:
# #     print(f"[ERROR] Bulk delete: {e}")

if __name__ == "__main__":
    main()
