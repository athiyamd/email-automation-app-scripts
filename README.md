# Finance Email Automation Template 

This project automates the generation and delivery of finance-related emails based on data in Google Sheets and CSV files from Google Drive. It's designed to streamline processes like:

- Invoice due reminders
- Scheduled client follow-ups
- Custom email body creation based on templates
- PDF attachment matching and sending
- Logging and tracking of sent emails

It uses:
- Google Drive API to fetch CSVs and attachments
- Google Sheets API to read project and log data
- Pandas for data cleaning and merging
- Google Apps Script / Gmail API to send dynamic emails

---

### Configurable
All columns, worksheet names, and merge keys are defined in a `config.py` file, making it easy to adapt this template for other projects or clients.

---

### Use Cases
- Finance teams sending recurring invoice reminders
- Operations handling multiple project-specific reports
- Automated reporting pipelines using Google Workspace tools

---
