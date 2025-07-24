import pandas as pd
from read_data import (
    creds, read_latest_csv_from_drive, read_sheet_to_df,
    map_attachments_by_invoice_code, sync_dataframe_to_sheet
)
from config import *

# --- Load CSV ---
csv_df = read_latest_csv_from_drive(csv_folder_id)
csv_df.columns = csv_df.columns.str.strip()

# --- Filter essential columns ---
csv_df = csv_df[required_csv_columns]

# --- Load attachments ---
attachments_map = map_attachments_by_invoice_code(attachment_folder_id)

# --- Merge with project sheets ---
merged_frames = []
for project in project_worksheets:
    if project not in csv_df[project_merge_keys[0]].unique():
        continue

    try:
        sheet_df = read_sheet_to_df(master_sheet_name, project)
        sheet_df.columns = sheet_df.columns.str.strip()
        selected_sheet = sheet_df[project_column_subset]

        merged = pd.merge(
            csv_df[csv_df[project_merge_keys[0]] == project],
            selected_sheet,
            on=project_merge_keys,
            how="left"
        )
        merged_frames.append(merged)
    except Exception as e:
        print(f"Skipping project '{project}': {e}")

merged_df = pd.concat(merged_frames, ignore_index=True) if merged_frames else pd.DataFrame()

# --- Transform ---
def enrich(df):
    today = pd.Timestamp.now(tz='Asia/Jakarta').normalize()
    
    # Convert to datetime and localize
    df[due_date_column] = pd.to_datetime(df[due_date_column], utc=True, errors='coerce').dt.tz_convert('Asia/Jakarta')
    
    # Day difference
    df[days_diff_column] = df[due_date_column].apply(
        lambda d: (d.normalize() - today).days if pd.notna(d) else None
    )

    # Body template mapping
    df[body_template_column] = df[days_diff_column].apply(
        lambda diff: email_template_map.get(diff, '')
    )

    # Format amount
    df[formatted_amount_column] = df[amount_column].apply(
        lambda x: f"Rp{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )

    # Body parameters string
    df[body_param_column] = df.apply(
        lambda r: param_delimiter.join([
            str(r.get(col, '')) if not pd.isna(r.get(col)) else ''
            for col in body_param_fields
        ]),
        axis=1
    )

    # Email subject from template
    df[email_subject_column] = df[body_template_column].map(email_subject_map)

    # Send date logic
    df[send_date_column] = df.apply(lambda r: (
        (r[due_date_column] + pd.Timedelta(days=send_offset_map.get(r[days_diff_column], 0))).strftime('%Y-%m-%d')
        if r[body_template_column] in email_subject_map
        else ''
    ), axis=1)

    # Attachments
    df[attachment_column] = df[attachment_key_column].apply(
        lambda code: attachment_delimiter.join(attachments_map.get(code, []))
    )

    return df

final_df = enrich(merged_df) if not merged_df.empty else pd.DataFrame(columns=final_output_columns)

# --- Save to Sheet ---
sync_dataframe_to_sheet(
    final_df,
    output_sheet_name,
    output_worksheet_name,
    unique_keys=project_merge_keys
)
