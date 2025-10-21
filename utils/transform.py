import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data(df: pd.DataFrame, exchange_rate: int = 16000) -> pd.DataFrame:
    """
    Membersihkan dan mentransformasi data mentah.
    REVISI 3: Menyesuaikan (1) Kapitalisasi header, (2) Membiarkan angka di title, (3) Membulatkan Price.
    """
    if df.empty:
        logging.warning("DataFrame input kosong, tidak ada transformasi yang dilakukan.")
        return df
        
    logging.info("Memulai proses transformasi data...")
    df_copy = df.copy()
    
    try:
        df_copy = df_copy[df_copy['Title'] != 'Unknown Product'].copy()

        df_copy['Price'] = df_copy['Price'].replace('Price Unavailable', np.nan)
        df_copy['Price'] = df_copy['Price'].str.replace(r'[$,]', '', regex=True).astype(float)
        df_copy['Price (IDR)'] = df_copy['Price'] * exchange_rate
        df_copy['Price (IDR)'] = df_copy['Price (IDR)'].round(0)

        df_copy['Rating'] = df_copy['Rating'].str.split(' ').str.get(2) 
        df_copy['Rating'] = df_copy['Rating'].replace('Invalid', np.nan)
        df_copy['Rating'] = pd.to_numeric(df_copy['Rating'], errors='coerce')
        df_copy['Rating'] = df_copy['Rating'].round(1) 

        df_copy['Colors'] = df_copy['Colors'].str.split(' ').str.get(0) 
        df_copy['Colors'] = df_copy['Colors'].replace('N/A', np.nan)
        df_copy['Colors'] = pd.to_numeric(df_copy['Colors'], errors='coerce')
        df_copy['Colors'] = df_copy['Colors'].fillna(1).astype(int)

        df_copy['Size'] = df_copy['Size'].str.replace('Size: ', '', regex=False)
        df_copy['Size'] = df_copy['Size'].str.strip(',') 
        df_copy['Size'] = df_copy['Size'].replace('N/A', np.nan)

        df_copy['Gender'] = df_copy['Gender'].str.replace('Gender: ', '', regex=False)
        df_copy['Gender'] = df_copy['Gender'].str.strip(',')
        df_copy['Gender'] = df_copy['Gender'].replace('N/A', np.nan)

        df_copy = df_copy.dropna(subset=['Title', 'Price (IDR)', 'Rating', 'Size', 'Gender'])

        df_copy = df_copy.drop_duplicates()

        df_copy['Price (IDR)'] = df_copy['Price (IDR)'].astype('float64')
        df_copy['Rating'] = df_copy['Rating'].astype('float64')
        df_copy['Colors'] = df_copy['Colors'].astype('int64')
        df_copy['Title'] = df_copy['Title'].astype('string')
        df_copy['Size'] = df_copy['Size'].astype('string')
        df_copy['Gender'] = df_copy['Gender'].astype('string')
        df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
        
        final_columns = ['Title', 'Price (IDR)', 'Rating', 'Colors', 'Size', 'Gender', 'timestamp']
        df_final = df_copy[final_columns]
        
        df_final.columns = ['title', 'Price', 'Rating', 'colors', 'size', 'gender', 'timestamp']
        
        logging.info(f"Transformasi selesai. {len(df_final)} data bersih tersisa.")
        return df_final
        
    except Exception as e:
        logging.error(f"Terjadi error saat transformasi data: {e}")
        return pd.DataFrame(columns=['title', 'Price', 'Rating', 'colors', 'size', 'gender', 'timestamp'])