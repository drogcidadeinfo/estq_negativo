import os
import glob
import gspread
import json
import time
import logging
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

# Config logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def get_latest_file(extension='xls', directory='.'):
    # Get the most recently modified file with a given extension.
    files = glob.glob(os.path.join(directory, f'*.{extension}'))
    if not files:
        logging.warning("No files found with the specified extension.")
        return None
    return max(files, key=os.path.getmtime)

def retry_api_call(func, retries=3, delay=2):
    for i in range(retries):
        try:
            return func()
        except HttpError as error:
            if hasattr(error, "resp") and error.resp.status == 500:
                logging.warning(f"APIError 500 encountered. Retrying {i + 1}/{retries}...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Max retries reached.")

def process_dataframe(df):
    df = df.iloc[:, 1:]  # Drop first column
    df = df.iloc[:, :-5]  # Drop last 5 columns

    filial = []
    filial_atual = None

    for _, row in df.iterrows():
        if row.iloc[0] == 'Filial:':
            filial_atual = row.iloc[2]
        else:
            filial.append(filial_atual if filial_atual else None)

    # Remove rows labeled 'Filial:'
    df = df[df.iloc[:, 0] != 'Filial:']

    # Ensure alignment
    if len(filial) != len(df):
        print(f"Length of filial: {len(filial)}, Length of DataFrame: {len(df)}")

    #ensure lenghts match
    df.loc[:, 'Filial'] = filial[:len(df)]
        
    # Remove rows where first column is NaN
    df = df[~df.iloc[:, 1].isna()]

    # Remove unnamed columns
    df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

    df = df.rename(columns={'Cód. ': 'Código'})
    df = df[['Código', 'Filial', ' Descrição Produto', 'Laboratório', 'Grupo', 'Curva/Padrão',
          'Estoq.\nMín.', 'Qtd.\nDem.', 'Est.\nCrit.', 'Acima\nDem/Crit', 'Qtd.\nEstoq.']]

    return df

def update_google_sheet(df, sheet_id):
    logging.info("Checking Google credentials environment variable...")
    creds_json = os.getenv("GGL_CREDENTIALS")
    if creds_json is None:
        logging.error("Google credentials not found in environment variables.")
        return

    creds_dict = json.loads(creds_json)
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(creds)
    print("Attempting to list spreadsheets...")
    for spreadsheet in client.openall():
        print("Found:", spreadsheet.title)

    # Open spreadsheet and worksheet
    try:
        spreadsheet = client.open_by_key(sheet_id)
        sheet = spreadsheet.worksheet("data")
    except Exception as e:
        logging.error(f"Error accessing spreadsheet: {e}")
        return

    # Prepare data
    logging.info("Preparing data for Google Sheets...")
    df = df.fillna("")  # Ensure no NaN values
    rows = [df.columns.tolist()] + df.values.tolist()

    # Clear sheet and update
    logging.info("Clearing existing data...")
    sheet.clear()
    logging.info("Uploading new data...")
    retry_api_call(lambda: sheet.update(rows))
    logging.info("Google Sheet updated successfully.")


def main():
    download_dir = '/home/runner/work/estq_negativo/estq_negativo/'
    latest_file = get_latest_file(directory=download_dir)
    sheet_id = os.getenv("SHEET_ID")

    if latest_file:
        logging.info(f"Loaded file: {latest_file}")
        try:
            df = pd.read_excel(latest_file, skiprows=11)
        except Exception as e:
            logging.error(f"Error reading Excel file: {e}")
            return

        processed_df = process_dataframe(df)
        # processed_df.to_excel("debug_processed_df.xlsx", index=False)

        if processed_df.empty:
            logging.warning("Processed DataFrame is empty. Skipping sheet update.")
            return

        update_google_sheet(processed_df, sheet_id)
    else:
        logging.warning("No new files to process.")


if __name__ == "__main__":
    main()
