import pytest
import requests_mock
import pandas as pd
from utils.extract import extract_data
from datetime import datetime

MOCK_HOME_PAGE_HTML = """
<html><body>
    <h1>Halaman Utama</h1>
</body></html>
"""

MOCK_PAGE_2_HTML = """
<html><body>
    <div class="collection-card">
        <div class="product-details">
            <h3 class="product-title">T-shirt 2</h3>
            <div class="price-container">$102.15</div>
            <p style="font-size: 14px;">Rating: ⭐ 3.9 / 5</p>
            <p style="font-size: 14px;">3 Colors</p>
            <p style="font-size: 14px;">Size: M,</p>
            <p style="font-size: 14px;">Gender: Women,</p>
        </div>
    </div>
    <div class="collection-card">
        <div class="product-details">
            <h3 class="product-title">Hoodie 3</h3>
            <div class="price-container">$496.88</div>
            <p style="font-size: 14px;">Rating: ⭐ 4.8 / 5</p>
            <p style="font-size: 14px;">3 Colors</p>
            <p style="font-size: 14px;">Size: L,</p>
            <p style="font-size: 14px;">Gender: Unisex,</p>
        </div>
    </div>
</body></html>
"""

MOCK_PAGE_3_HTML = """
<html><body>
    <h1>Tidak ada produk</h1>
</body></html>
"""

def test_extract_data(requests_mock):
    """Test fungsi extract_data (integrasi)."""
    base_url = "https://fashion-studio.dicoding.dev"

    requests_mock.get(f"{base_url}/", text=MOCK_HOME_PAGE_HTML)

    requests_mock.get(f"{base_url}/page2", text=MOCK_PAGE_2_HTML)

    requests_mock.get(f"{base_url}/page3", text=MOCK_PAGE_3_HTML)

    df = extract_data(base_url, total_pages=3)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert 'timestamp' in df.columns
    

    assert df.loc[0, 'Title'] == 'T-shirt 2'
    assert df.loc[0, 'Price'] == '$102.15'
    assert df.loc[0, 'Rating'] == 'Rating: ⭐ 3.9 / 5'
    assert df.loc[0, 'Colors'] == '3 Colors'
    assert df.loc[0, 'Size'] == 'Size: M,'
    assert df.loc[0, 'Gender'] == 'Gender: Women,'


    assert df.loc[1, 'Title'] == 'Hoodie 3'
    assert df.loc[1, 'Price'] == '$496.88'

def test_extract_data_stop_on_404(requests_mock):
    """Test bahwa ekstraksi berhenti jika halaman 404."""
    base_url = "https://fashion-studio.dicoding.dev"
    

    requests_mock.get(f"{base_url}/", text=MOCK_HOME_PAGE_HTML)
    requests_mock.get(f"{base_url}/page2", status_code=404)
    requests_mock.get(f"{base_url}/page3", text=MOCK_PAGE_2_HTML)


    df = extract_data(base_url, total_pages=3)
    
    assert df.empty 

def test_extract_data_no_data(requests_mock):
    """Test jika tidak ada data sama sekali."""
    base_url = "https://fashion-studio.dicoding.dev"
    requests_mock.get(f"{base_url}/", text=MOCK_HOME_PAGE_HTML)
    requests_mock.get(f"{base_url}/page2", text=MOCK_PAGE_3_HTML)

    df = extract_data(base_url, total_pages=2)
    assert df.empty