import io
import re
import pandas as pd
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# --- Google Authentication ---
creds, _ = default(scopes=[
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
])

# --- CSV Reader ---
def read_latest_csv_from_drive(folder_id: str) -> pd.DataFrame:
    """
    Download and return the most recent CSV file from a specified Google Drive folder.
    """
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
        raise FileNotFoundError("No CSV file found in the specified Drive folder.")

    latest_file = files[0]
    request = drive_service.files().get_media(fileId=latest_file['id'])
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    while not downloader.next_chunk()[1]:
        pass

    fh.seek(0)
    return pd.read_csv(fh)

# --- Sheet Reader ---
def read_sheet_to_df(sheet_name: str, worksheet_name: str) -> pd.DataFrame:
    """
    Read a specific worksheet from a Google Sheet and return as a DataFrame.
    """
    client = gspread.authorize(creds)
    worksheet = client.open(sheet_name).worksheet(worksheet_name)
    return get_as_dataframe(worksheet, evaluate_formulas=True)

# --- Attachment Mapper ---
def map_attachments_by_invoice_code(folder_id: str, key_pattern: str = r'(SI\d+)') -> dict:
    """
    Map keys (e.g., invoice codes) to file links in a Google Drive folder using regex.

    Args:
        folder_id (str): Google Drive folder ID
        key_pattern (str): Regex pattern to extract key from file name

    Returns:
        dict: { key: [list of webViewLinks] }
    """
    drive_service = build('drive', 'v3', credentials=creds)
    attachments = {}
    page_token = None

    while True:
        response = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, webViewLink)",
            pageToken=page_token
        ).execute()

        for file in response.get('files', []):
            matches = re.findall(key_pattern, file['name'])
            for match in matches:
                attachments.setdefault(match, []).append(file.get('webViewLink'))

        page_token = response.get('nextPageToken')
        if not page_token:
            break

    return attachments

# --- Sheet Syncer ---
def sync_dataframe_to_sheet(df: pd.DataFrame, sheet_name: str, worksheet_name: str, unique_keys: list[str]):
    """
    Append only new or updated rows to a Google Sheet, based on unique key comparison.

    Args:
        df (pd.DataFrame): Final DataFrame to sync
        sheet_name (str): Name of the target spreadsheet
        worksheet_name (str): Worksheet to update
        unique_keys (list[str]): List of column names to generate row-level unique keys
    """
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).worksheet(worksheet_name)
    existing_df = get_as_dataframe(sheet, evaluate_formulas=True).dropna(how='all')

    df.columns = df.columns.str.strip()
    existing_df.columns = existing_df.columns.str.strip()

    # Ensure consistent column order
    missing_cols = [col for col in df.columns if col not in existing_df.columns]
    for col in missing_cols:
        existing_df[col] = None  # Add missing columns to existing sheet
    df = df[existing_df.columns.tolist()]

    def generate_key(dframe):
        return dframe[unique_keys].astype(str).agg('|'.join, axis=1)

    existing_df['_key'] = generate_key(existing_df)
    df['_key'] = generate_key(df)

    new_rows = df[~df['_key'].isin(existing_df['_key'])]

    if not new_rows.empty:
        print(f"Appending {len(new_rows)} new rows...")
        next_row = len(existing_df) + 2
        set_with_dataframe(sheet, new_rows.drop(columns=['_key']), row=next_row, include_column_header=False)
    else:
        print("No new rows to sync.")
