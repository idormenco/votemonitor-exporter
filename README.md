# Vote Monitor Exporter

This project downloads election form submissions, quick reports, and related attachments from the VoteMonitor API and exports them into a structured Excel file and an Sqlite Db

## Prerequisites

- **Python 3.9+** (recommended)
- `pip` package manager
- Access to the VoteMonitor API (with valid credentials)

## Setup

1. **Clone this repository** (or download the files):
   ```bash
   git clone https://github.com/idormenco/votemonitor-exporter.git
   cd votemonitor-export
2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   # On Linux / macOS
   source venv/bin/activate
   # On Windows
   venv\Scripts\activate
3. Install dependencies
   ```bash
   pip install -r requirements.txt
4. Create `.env` file
   ```bash
   cp .env.example .env
5. Configure the .env file
   ```dotenv
   DB_FILE=<name-of-sqlite-db>
   BASE_API_URL=<api-url>
   ADMIN_EMAIL=<admin-email>
   ADMIN_PASSWORD=<admin-password>
   ELECTION_ID=<election-id>
   DOWNLOAD_ATTACHMENTS=false | true
6. Running the script

   Please run the script at least every **15 minutes**
   ```bash
   python main.py
   ```
   Exported data is located in `exported-data` folder

