# Finance Email Automation Template

This project automates the generation and delivery of finance-related emails using data from Google Sheets and CSV files stored in Google Drive. It's designed to streamline recurring communication workflows such as:

- Invoice due reminders  
- Scheduled client follow-ups  
- Dynamic email content using templates  
- PDF attachment matching and delivery  
- Centralized email logging and tracking

---

## Features

- Fetches the latest CSV data from Google Drive
- Merges with project-specific data in Google Sheets
- Applies logic for email templates, subjects, and due dates
- Sends dynamic emails via Gmail (with optional attachments)
- Logs results in a shared Google Sheet for monitoring

---

## Tech Stack

- **Google Drive API** – to read CSVs and retrieve attachments  
- **Google Sheets API** – to fetch project data and update logs  
- **Pandas** – for data cleaning, merging, and formatting  
- **Gmail API / Google Apps Script** – to send personalized emails  

---

## Requirements

- Python 3.7+
- Google Cloud SDK (with Application Default Credentials set up)
- Required libraries (install with pip):

```bash
pip install pandas gspread google-auth google-api-python-client gspread-dataframe
