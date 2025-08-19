Email Ticketing System

A Python-based Email Ticketing System that fetches emails from a POP3 server, extracts details, stores them in a SQL Server database, and saves attachments. It tracks membership info, generates tickets, detects undelivered emails, and logs errors. Configure using .env and creds.txt.

Project File Structure
email-ticketing-system/
│
├── src/                              # Source code directory
│   ├── main.py                       # Main script to run the system
│   ├── utils/                        # (Optional) Helper functions
│   └── __init__.py
│
├── config/
│   ├── .env.example                  # Example environment variables file (copy to .env)
│   ├── creds.example.txt             # Example credentials file (copy to creds.txt)
│   └── .gitignore                    # Git ignore file to prevent sensitive files from being tracked
│
├── attachments/                      # Directory to store email attachments
├── ticket_uuids.json                 # Logs UUIDs linked to tickets
├── seen_uidls.json                   # UIDL cache file
├── requirements.txt                  # List of dependencies
├── README.md                         # Project documentation
└── LICENSE                           # Project license (MIT)

Setup and Installation

Clone the repository:

git clone https://github.com/yourusername/email-ticketing-system.git
cd email-ticketing-system


Install dependencies:
Create a virtual environment and install the required libraries:

python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt


Create environment file:
Copy the .env.example and creds.example.txt to .env and creds.txt, respectively, and fill in the required values like your POP3 credentials, database connection info, and any other necessary details.

cp .env.example .env
cp creds.example.txt creds.txt


Run the system:
After configuration, run the system:

python src/main.py

Notes

.env: This file contains sensitive information like POP3 and database credentials. It should not be shared publicly.

creds.txt: Contains credentials used by the system. Ensure this file is kept secure and not uploaded to public repositories.
