import io
import re
import pandas as pd
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from datetime import datetime, timedelta, timezone

# ---- Use ADC ----
creds, _ = default(scopes=[
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
])

def read_csv_from_drive(folder_id):
    """Fetch the latest CSV file from a Google Drive folder."""
    drive_service = build('drive', 'v3', credentials=creds)

    query = f"'{folder_id}' in parents and mimeType='text/csv'"
    response = drive_service.files().list(
        q=query,
        orderBy='createdTime desc',
        pageSize=1,
        fields='files(id, name, createdTime)'
    ).execute()

    files = response.get('files', [])
    if not files:
        raise FileNotFoundError(f"No CSV files found in folder '{folder_id}'")

    latest_file = files[0]
    print(f"Reading latest file: {latest_file['name']} (Created: {latest_file['createdTime']})")

    request = drive_service.files().get_media(fileId=latest_file['id'])
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    while not downloader.next_chunk()[1]:
        pass

    fh.seek(0)
    df = pd.read_csv(fh)

    if 'Due At' in df.columns:
        today = pd.Timestamp.now(tz='Asia/Jakarta')

        # Ensure 'Due At' is parsed correctly
        df['Due At'] = pd.to_datetime(df['Due At'], dayfirst=True, errors='coerce', utc=True)
        df['Due At'] = df['Due At'].dt.tz_convert('Asia/Jakarta')

        # Compute Day Diff using normalized dates
        df['Day Diff'] = (df['Due At'].dt.normalize() - today.normalize()).dt.days

        # Filter the range
        df = df[df['Day Diff'].isin([-14, -7, 7])].copy()

        if df.empty:
            print("No rows matched Day Diff in [-14, -7, 7]. Returning empty DataFrame.")
        else:
            print(f"Returning filtered data with {len(df)} rows.")

    else:
        print("Warning: 'Due At' column not found, no date filter applied.")
    return df

def read_google_sheet(sheet_name, worksheet_name):
    """Reads data from a worksheet in a Google Sheet."""
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).worksheet(worksheet_name)
    return get_as_dataframe(sheet, evaluate_formulas=True)

def get_attachments_map(folder_id):
    """Builds a map of invoice code → list of Drive attachment links."""
    drive_service = build('drive', 'v3', credentials=creds)
    attachments_map = {}
    page_token = None

    while True:
        response = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, webViewLink)",
            pageToken=page_token
        ).execute()

        for file in response.get('files', []):
            name = file['name']
            link = file.get('webViewLink')
            matches = re.findall(r'(SI\d+)', name)
            for code in matches:
                attachments_map.setdefault(code, []).append(link)

        page_token = response.get('nextPageToken')
        if not page_token:
            break

    return attachments_map

def sync_to_google_sheet(final_df, sheet_name, worksheet_name, creds):
    """
    Append only new or updated rows to a worksheet using 'Sales Invoice Code' + 'Body Template' as a unique key.
    """
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).worksheet(worksheet_name)

    # Read current data
    existing_df = get_as_dataframe(sheet, evaluate_formulas=True).dropna(how='all')
    existing_df.columns = existing_df.columns.str.strip()

    print(type(final_df))  # Should be <class 'pandas.core.frame.DataFrame'>

    final_df.columns = final_df.columns.str.strip()
    final_df = final_df[existing_df.columns.tolist()]  # Reorder columns to match

    def gen_key(df):
        return df['Sales Invoice Code'].astype(str) + '|' + df['Body Template'].astype(str)

    existing_df['_key'] = gen_key(existing_df)
    # final_df['_key'] = gen_key(final_df)
    final_df.loc[:, '_key'] = gen_key(final_df)


    new_or_changed_rows = final_df[~final_df['_key'].isin(existing_df['_key'])]

    if new_or_changed_rows.empty:
        print("No new/updated rows to sync.")
        return

    print(f"Appending {len(new_or_changed_rows)} new/updated rows...")

    next_row = len(existing_df) + 2  # +2 accounts for header row
    set_with_dataframe(
        sheet,
        new_or_changed_rows.drop(columns=['_key']),
        row=next_row,
        include_column_header=False
    )