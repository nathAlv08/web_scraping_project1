import os
import logging
from dotenv import load_dotenv
from utils.extract import extract_data
from utils.transform import transform_data
from utils.load import load_to_csv, load_to_gdrive, load_to_postgres

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Fungsi utama untuk menjalankan pipeline ETL."""
    
    load_dotenv()
    
    DB_URL = os.getenv('DB_URL')
    GSHEET_ID = os.getenv('GSHEET_ID')
    SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
    CSV_FILE_PATH = os.getenv('CSV_FILE_PATH')
    DB_TABLE_NAME = os.getenv('DB_TABLE_NAME')
    
    if not all([DB_URL, GSHEET_ID, SERVICE_ACCOUNT_FILE, CSV_FILE_PATH, DB_TABLE_NAME]):
        logging.error("Satu atau lebih variabel lingkungan (.env) tidak diatur. Pipeline berhenti.")
        return

    BASE_URL = "https://fashion-studio.dicoding.dev"
    TOTAL_PAGES = 50

    logging.info("Memulai ETL Pipeline...")

    logging.info("="*30)
    logging.info("[1/3] Memulai Tahap Extract...")
    try:
        raw_df = extract_data(BASE_URL, TOTAL_PAGES)
        if raw_df.empty:
            logging.warning("Ekstraksi tidak menghasilkan data. Pipeline berhenti.")
            return
        logging.info(f"Extract Selesai. {len(raw_df)} data mentah didapat.")
    except Exception as e:
        logging.error(f"Error besar pada Tahap Extract: {e}")
        return

    logging.info("="*30)
    logging.info("[2/3] Memulai Tahap Transform...")
    try:
        cleaned_df = transform_data(raw_df)
        if cleaned_df.empty:
            logging.warning("Transformasi tidak menghasilkan data (mungkin semua data invalid). Pipeline berhenti.")
            return
        logging.info(f"Transform Selesai. {len(cleaned_df)} data bersih siap di-load.")
        logging.info("Contoh data bersih:")
        logging.info(f"\n{cleaned_df.head()}")
        logging.info(f"\nInfo Tipe Data:\n{cleaned_df.info()}")
    except Exception as e:
        logging.error(f"Error besar pada Tahap Transform: {e}")
        return

    logging.info("="*30)
    logging.info("[3/3] Memulai Tahap Load...")

    load_to_csv(cleaned_df, CSV_FILE_PATH)

    load_to_gdrive(cleaned_df, GSHEET_ID, SERVICE_ACCOUNT_FILE)

    load_to_postgres(cleaned_df, DB_URL, DB_TABLE_NAME)
    
    logging.info("="*30)
    logging.info("ETL Pipeline Selesai.")

if __name__ == "__main__":
    main()