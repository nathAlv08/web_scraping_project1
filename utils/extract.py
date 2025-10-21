import requests
from requests.exceptions import HTTPError  
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data(base_url: str, total_pages: int = 50) -> pd.DataFrame:
    """
    Fungsi utama untuk extract data.
    Menggunakan selector yang benar berdasarkan 'Inspect Element' dari user.
    """
    all_products = []
    extraction_timestamp = datetime.now()
    
    logging.info(f"Memulai ekstraksi data dari {base_url} untuk {total_pages} halaman.")
    
    for page in range(1, total_pages + 1):

        if page == 1:
            shop_url = f"{base_url}/"
        else:
            shop_url = f"{base_url}/page{page}"

        logging.info(f"Scraping halaman: {page}/{total_pages} - {shop_url}")
        
        try:
            response = requests.get(shop_url, timeout=10)
            response.raise_for_status() 
            soup = BeautifulSoup(response.text, 'html.parser')
            
            product_cards = soup.select('div.collection-card')
            
            if not product_cards:
                if page == 1:
                    logging.warning(f"Tidak ada 'div.collection-card' di Halaman 1. Melanjutkan...")
                    continue 
                else:
                    logging.warning(f"Tidak ada 'div.collection-card' di halaman {page}. Berhenti.")
                    break
            
            logging.info(f"Menemukan {len(product_cards)} produk di halaman {page}.")

            for card in product_cards:
                
                title_tag = card.find('h3', class_='product-title')
                title = title_tag.get_text(strip=True) if title_tag else 'Unknown Product'
                
                price_tag = card.find(class_='price-container')
                price = price_tag.get_text(strip=True) if price_tag else 'Price Unavailable'
                
                rating_raw = 'Invalid Rating'
                colors_raw = 'N/A'
                size_raw = 'N/A'
                gender_raw = 'N/A'
                
                details_div = card.find('div', class_='product-details')
                if details_div:
                    p_tags = details_div.find_all('p')
                    for p in p_tags:
                        text = p.get_text(strip=True)
                        if text.startswith('Rating:'):
                            rating_raw = text
                        elif 'Colors' in text and text.endswith('Colors'):
                            colors_raw = text
                        elif text.startswith('Size:'):
                            size_raw = text
                        elif text.startswith('Gender:'):
                            gender_raw = text

                all_products.append({
                    'Title': title,
                    'Price': price,
                    'Rating': rating_raw,
                    'Colors': colors_raw,
                    'Size': size_raw,
                    'Gender': gender_raw,
                    'timestamp': extraction_timestamp
                })

        except HTTPError as e:
            if e.response.status_code == 404:
                logging.warning(f"Halaman {page} tidak ditemukan (404). Berhenti.")
                break  
                logging.error(f"HTTP Error Gagal mengambil halaman {page}: {e}")
                continue
        except requests.RequestException as e:
            logging.error(f"Request Gagal mengambil halaman {page}: {e}")
            continue 
        except Exception as e:
            logging.error(f"Error tidak terduga saat parsing halaman {page}: {e}")
            continue
            
    logging.info(f"Ekstraksi selesai. Total {len(all_products)} data mentah didapat.")
    
    if not all_products:
        logging.warning("Tidak ada data yang berhasil diekstrak.")
        return pd.DataFrame()
            
    return pd.DataFrame(all_products)