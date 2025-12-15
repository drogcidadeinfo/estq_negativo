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

def create_empty_dataframe():
    """Create an empty DataFrame with expected columns filled with '-'"""
    expected_columns = [
        'Código', 
        'Filial', 
        'Descrição Produto',
        'Laboratório', 
        'Grupo', 
        'Curva/Padrão',
        'Estoq.\nMín.', 
        'Qtd.\nDem.', 
        'Est.\nCrit.', 
        'Acima\nDem/Crit', 
        'Qtd.\nEstoq.'
    ]
    
    # Create a DataFrame with one row filled with '-'
    df = pd.DataFrame({col: ['-'] for col in expected_columns})
    
    logging.info("Created empty DataFrame with expected columns filled with '-'")
    return df

def process_dataframe(df):
    # Check if the DataFrame is empty or has very few rows/columns
    # This indicates the download had no data
    if df.empty or df.shape[1] < 5 or df.shape[0] < 5:
        logging.warning("DataFrame appears to be empty or have insufficient data. Creating empty structure.")
        return create_empty_dataframe()
    
    try:
        # Try to process normally first
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

        # Ensure lengths match
        df.loc[:, 'Filial'] = filial[:len(df)]
            
        # Remove rows where first column is NaN
        df = df[~df.iloc[:, 1].isna()]

        # Remove unnamed columns
        df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

        # Try to rename columns, handling different possible column names
        column_renames = {}
        for col in df.columns:
            col_str = str(col).strip()
            if 'Cód' in col_str:
                column_renames[col] = 'Código'
            elif 'Descrição' in col_str or 'Produto' in col_str:
                column_renames[col] = ' Descrição Produto'
            elif 'Laboratório' in col_str:
                column_renames[col] = 'Laboratório'
            elif 'Grupo' in col_str:
                column_renames[col] = 'Grupo'
            elif 'Curva' in col_str or 'Padrão' in col_str or 'Padrao' in col_str:
                column_renames[col] = 'Curva/Padrão'
            elif 'Estoq' in col_str and 'Mín' in col_str:
                column_renames[col] = 'Estoq.\nMín.'
            elif 'Qtd' in col_str and 'Dem' in col_str:
                column_renames[col] = 'Qtd.\nDem.'
            elif 'Est' in col_str and 'Crit' in col_str:
                column_renames[col] = 'Est.\nCrit.'
            elif 'Acima' in col_str and ('Dem' in col_str or 'Crit' in col_str):
                column_renames[col] = 'Acima\nDem/Crit'
            elif 'Qtd' in col_str and 'Estoq' in col_str:
                column_renames[col] = 'Qtd.\nEstoq.'
        
        if column_renames:
            df = df.rename(columns=column_renames)
        
        # Check if we have the expected columns
        expected_columns = [
            'Código', 
            'Filial', 
            ' Descrição Produto', 
            'Laboratório', 
            'Grupo', 
            'Curva/Padrão',
            'Estoq.\nMín.', 
            'Qtd.\nDem.', 
            'Est.\nCrit.', 
            'Acima\nDem/Crit', 
            'Qtd.\nEstoq.'
        ]
        
        missing_columns = [col for col in expected_columns if col not in df.columns]
        
        if missing_columns:
            logging.warning(f"Missing expected columns after processing: {missing_columns}")
            logging.warning("Available columns: {list(df.columns)}")
            
            # If we're missing many columns, the file might be empty
            if len(missing_columns) > 5:
                logging.warning("Too many columns missing. Creating empty DataFrame.")
                return create_empty_dataframe()
            
            # Add missing columns with '-'
            for col in missing_columns:
                df[col] = '-'
        
        # Try to select the expected columns
        try:
            df = df[expected_columns]
        except KeyError as e:
            logging.error(f"Cannot select expected columns. Error: {e}")
            logging.error(f"Available columns: {list(df.columns)}")
            logging.warning("Creating empty DataFrame instead.")
            return create_empty_dataframe()
        
        return df
        
    except Exception as e:
        logging.error(f"Error processing DataFrame: {e}")
        logging.warning("Creating empty DataFrame due to processing error.")
        return create_empty_dataframe()

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
            # Try reading with different row skips if needed
            try:
                df = pd.read_excel(latest_file, skiprows=11)
            except:
                # If that fails, try reading without skipping rows
                df = pd.read_excel(latest_file)
                logging.info("Read file without skipping rows.")
            
            # Check if the DataFrame is essentially empty
            if df.empty or (df.shape[0] < 3 and df.shape[1] < 3):
                logging.warning("File appears to be empty or have no data.")
                processed_df = create_empty_dataframe()
            else:
                processed_df = process_dataframe(df)
            
        except Exception as e:
            logging.error(f"Error reading Excel file: {e}")
            # Create empty DataFrame on read error
            processed_df = create_empty_dataframe()

        # Save for debugging
        processed_df.to_excel("debug_processed_df.xlsx", index=False)

        if processed_df.empty:
            logging.warning("Processed DataFrame is empty. Creating single row with dashes.")
            processed_df = create_empty_dataframe()

        update_google_sheet(processed_df, sheet_id)
    else:
        logging.warning("No new files to process.")
        # Optionally, you could create an empty file here if needed


if __name__ == "__main__":
    main()
