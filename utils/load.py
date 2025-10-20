import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_to_csv(df: pd.DataFrame, file_path: str) -> None:
    """
    Menyimpan DataFrame ke file CSV. (Kriteria 2 - Basic)
    
    Args:
        df (pd.DataFrame): DataFrame bersih.
        file_path (str): Lokasi file CSV output.
    """
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
        logging.info(f"Data berhasil disimpan ke CSV: {file_path}")
    except (IOError, OSError) as e:
        logging.error(f"Gagal menyimpan ke CSV {file_path}: {e}")
        
def load_to_gdrive(df: pd.DataFrame, sheet_id: str, creds_path: str) -> None:
    """
    Mengupload DataFrame ke Google Sheets. (Kriteria 2 - Skilled/Advanced)
    
    Args:
        df (pd.DataFrame): DataFrame bersih.
        sheet_id (str): ID Google Sheet.
        creds_path (str): Path ke file google-sheets-api.json.
    """
    if not os.path.exists(creds_path):
        logging.error(f"File kredensial Google Sheets tidak ditemukan: {creds_path}")
        return
        
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)

        df_gdrive = df.copy()
        df_gdrive['timestamp'] = df_gdrive['timestamp'].astype(str)
        
        values = [df_gdrive.columns.tolist()] + df_gdrive.values.tolist()
        
        body = {
            'values': values
        }

        range_name = 'Sheet1!A1'

        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range='Sheet1'
        ).execute()

        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        
        logging.info(f"Data berhasil di-upload ke Google Sheet ID: {sheet_id}")
        
    except HttpError as e:
        logging.error(f"Error saat API Google Sheets: {e}")
    except Exception as e:
        logging.error(f"Error tidak terduga saat upload ke Google Sheets: {e}")

def load_to_postgres(df: pd.DataFrame, db_url: str, table_name: str) -> None:
    """
    Menyimpan DataFrame ke database PostgreSQL. (Kriteria 2 - Skilled/Advanced)
    
    Args:
        df (pd.DataFrame): DataFrame bersih.
        db_url (str): URL koneksi SQLAlchemy.
        table_name (str): Nama tabel tujuan.
    """
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            
            df.to_sql(table_name, connection, if_exists='replace', index=False)
            logging.info(f"Data berhasil disimpan ke PostgreSQL, tabel: {table_name}")
            
    except SQLAlchemyError as e:
        logging.error(f"Gagal menyimpan ke PostgreSQL: {e}")
    except Exception as e:
        logging.error(f"Error tidak terduga saat koneksi PostgreSQL: {e}")