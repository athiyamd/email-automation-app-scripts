# === Google Drive & Sheet Configuration ===

# ID of the Google Drive folder containing the latest CSV export
csv_folder_id = "your-csv-folder-id"

# ID of the Drive folder containing the attachments to be matched
attachment_folder_id = "your-attachment-folder-id"

# Name of the Google Spreadsheet that holds all worksheet data
master_sheet_name = "your-master-sheet-name"

# List of worksheet names (tabs) to include in the merging process
project_worksheets = ["Project A", "Project B"]

# Unique column keys used to merge sheet and CSV data (e.g., invoice ID)
project_merge_keys = ["your-merge-key-column"]

# Subset of project columns to retain (optional; for column filtering)
project_column_subset = [
    "your-merge-key-column",
    "your-column-1", 
    "your-column-2", 
    "etc."
]

# Columns expected in the CSV file to ensure correct structure
required_csv_columns = [
    "your-merge-key-column", 
    "your-column-1", 
    "your-column-2", 
    "etc."
]

# Final output Google Sheet & worksheet
output_sheet_name = "your-output-sheet-name"
output_worksheet_name = "your-output-worksheet-name"

# Columns in the final output DataFrame (used in export to sheet)
final_output_columns = [
    "your-column-1", 
    "your-column-2", 
    "etc."
]

# === Columns Configuration ===

amount_column = "your-amount-column"
due_date_column = "your-due-date-column"
days_diff_column = "your-days-diff-column"
formatted_amount_column = "your-formatted-amount-column"
body_template_column = "your-body-template-column"
body_param_column = "your-body-param-column"
email_subject_column = "your-email-subject-column"
send_date_column = "your-send-date-column"
attachment_column = "your-attachment-column"
attachment_key_column = "your-attachment-lookup-key"

# === Email Template & Logic ===

# Map day differences to template names
# --- Define how days should be mapped based on your specific case ---
email_template_map = {
    7: 'Template-1',
    -7: 'Template-2',
    -14: 'Template-3'
}

# Map template names to email subject lines
email_subject_map = {
    'Template-1': 'Reminder: Payment Due',
    'Template-2': 'Finance-SP 1',
    'Template-3': 'Finance-SP 2'
}

# Map day differences to send-date offsets (e.g., send 7 days before due)
send_offset_map = {
    7: -7,
    -7: 7,
    -14: 14
}

# === Body Parameter Configuration ===

# List of column names to extract as dynamic body params
body_param_fields = [
    "your-body-param-col-1",
    "your-body-param-col-2",
    "your-body-param-col-3",
    "your-body-param-col-4"
]

# Delimiters used for joining values in body/attachment fields
param_delimiter = ";"
attachment_delimiter = ";"
