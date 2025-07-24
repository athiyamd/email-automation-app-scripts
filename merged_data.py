import pandas as pd
from read_data import (
    creds,
    read_csv_from_drive, 
    read_google_sheet, 
    get_attachments_map, 
    sync_to_google_sheet
)

# --- CONFIGURATION ---
csv_folder_id = '1lHIdqp1QE9H682G4pkUbBNRjon8NlhZu'
sheet_name = 'master_data'
attachment_folder_id = '1qoUBN1J5e0sOWjNxTCCI58p-JFbW8VtZ'
project_worksheets = ["Data", "GWS", "GCP", "AWS", "Digital Marketing"]

# --- Read Source Data ---
attachments_map = get_attachments_map(attachment_folder_id)

csv_df = read_csv_from_drive(csv_folder_id)
csv_df.columns = csv_df.columns.str.strip()
# print(csv_df.head())
csv_df = csv_df[[
    "Customer Name", "Project Name", "Business Unit Name",
    "Sales Invoice Code", "Due At", "Receivable Amount"
]]

merged_list = []

for project in project_worksheets:
    if project not in csv_df["Project Name"].unique():
        continue

    try:
        sheet_df = read_google_sheet(sheet_name, project)
        sheet_df.columns = sheet_df.columns.str.strip()
        sheet_selected = sheet_df[[
            "Customer Name", "Project Name", "Business Unit Name",
            "Email To:", "Email CC:"
        ]]

        csv_filtered = csv_df[csv_df["Project Name"] == project]

        merged_df = pd.merge(
            csv_filtered,
            sheet_selected,
            on=["Customer Name", "Project Name", "Business Unit Name"],
            how="left"
        )

        merged_list.append(merged_df)

    except Exception as e:
        print(f"Error reading worksheet '{project}': {e}")
        continue

merged_df = pd.concat(merged_list, ignore_index=True) if merged_list else pd.DataFrame()

# --- Transformation ---
if not merged_df.empty and 'Due At' in merged_df.columns:
    merged_df['Due At'] = pd.to_datetime(merged_df['Due At'], utc=True, errors='coerce').dt.tz_convert('Asia/Jakarta')
    merged_df['Due At_raw'] = merged_df['Due At']

    today = pd.Timestamp.now(tz='Asia/Jakarta').normalize()

    merged_df['Days Diff'] = merged_df['Due At_raw'].apply(
        lambda d: (d.normalize() - today).days if pd.notna(d) else None
    )

    def get_template(due_date):
        if pd.isna(due_date): return ''
        diff = (due_date.normalize() - today).days
        if diff == 7: return 'Template-1'
        elif diff == -7: return 'Template-2'
        elif diff == -14: return 'Template-3'
        return ''

    merged_df['Body Template'] = merged_df['Due At_raw'].apply(get_template)

    merged_df["Receivable Amount Formatted"] = merged_df["Receivable Amount"].apply(
        lambda x: f"Rp{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )

    merged_df['Body Params'] = merged_df.apply(
        lambda row: 
            f"{row['Customer Name']};{row['Sales Invoice Code']};{row['Receivable Amount Formatted']};{row['Due At_raw'].strftime('%d-%m-%Y') if pd.notna(row['Due At_raw']) else ''}",
        axis=1
    )

    def get_email_subject(row):
        diff = row['Days Diff']
        template = row['Body Template']
        if pd.isna(diff): return ''
        if template == 'Template-1': return "Reminder: Payment Due"
        elif template == 'Template-2': return "Finance-SP 1"
        elif template == 'Template-3': return "Finance-SP 2"
        return ''

    merged_df['Email Subject'] = merged_df.apply(get_email_subject, axis=1)
    merged_df['Receiver To'] = merged_df['Email To:']
    merged_df['Receiver Cc'] = merged_df['Email CC:']
    merged_df['Due At'] = merged_df['Due At_raw'].dt.strftime('%Y-%m-%d')

    def get_send_date(row):
        if pd.isna(row['Due At_raw']): return ''
        days_diff = row['Days Diff']
        if row['Body Template'] == 'Template-1' and days_diff == 7:
            return (row['Due At_raw'] - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
        elif row['Body Template'] == 'Template-2' and days_diff == -7:
            return (row['Due At_raw'] + pd.Timedelta(days=7)).strftime('%Y-%m-%d')
        elif row['Body Template'] == 'Template-3' and days_diff == -14:
            return (row['Due At_raw'] + pd.Timedelta(days=14)).strftime('%Y-%m-%d')
        return ''

    merged_df['Send Date'] = merged_df.apply(get_send_date, axis=1)
    merged_df['Attachments'] = merged_df['Sales Invoice Code'].apply(
        lambda code: ';'.join(attachments_map.get(code, []))
    )

else:
    print("merged_df is empty or missing 'Due At'. Skipping all transformation steps.")

expected_cols = [
    'Sales Invoice Code', 'Email Subject', 'Body Template',
    'Body Params', 'Receiver To', 'Receiver Cc',
    'Due At', 'Send Date', 'Attachments'
]

# final_df = merged_df[merged_df.columns.intersection(expected_cols)].copy() \
#     if not merged_df.empty else pd.DataFrame(columns=expected_cols)

if not merged_df.empty:
    final_df = merged_df[merged_df.columns.intersection(expected_cols)].copy()
    print(final_df._is_copy) #Check if it's not a view, but a DataFrame
else:
    final_df = pd.DataFrame(columns=expected_cols)


sync_to_google_sheet(
    final_df=final_df,
    sheet_name="Finance-Dev",
    worksheet_name="Email Record",
    creds=creds
)
