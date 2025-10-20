import pytest
import requests_mock
import pandas as pd
from utils.extract import extract_data, scrape_product_details
from datetime import datetime

MOCK_SHOP_PAGE_HTML = """
<html><body>
    <li class="product">
        <h2 class="woocommerce-loop-product__title">
            <a href="https://fashion-studio.dicoding.dev/index.php/product/product-1/">Product 1</a>
        </h2>
    </li>
    <li class="product">
        <h2 class="woocommerce-loop-product__title">
            <a href="https://fashion-studio.dicoding.dev/index.php/product/product-2/">Product 2</a>
        </h2>
    </li>
</body></html>
"""

MOCK_PRODUCT_1_HTML = """
<html><body>
    <h1 class="product_title">Product 1</h1>
    <p class="price">$50.00</p>
    <div class="star-rating" aria-label="Rated 4.50 out of 5"></div>
    <table class="woocommerce-product-attributes">
        <tr><th class="woocommerce-product-attributes-item__label">Color</th>
            <td class="woocommerce-product-attributes-item__value">Blue, Red</td></tr>
        <tr><th class="woocommerce-product-attributes-item__label">Size</th>
            <td class="woocommerce-product-attributes-item__value">M, L</td></tr>
        <tr><th class="woocommerce-product-attributes-item__label">Gender</th>
            <td class="woocommerce-product-attributes-item__value">Men</td></tr>
    </table>
</body></html>
"""

MOCK_PRODUCT_2_HTML_INVALID = """
<html><body>
    <h1 class="product_title">Unknown Product</h1>
    <p class="price">Price Unavailable</p>
    <div class="star-rating">Invalid Rating</div>
    <table class="woocommerce-product-attributes">
        <tr><th class="woocommerce-product-attributes-item__label">Color</th>
            <td class="woocommerce-product-attributes-item__value">N/A</td></tr>
        <tr><th class="woocommerce-product-attributes-item__label">Size</th>
            <td class="woocommerce-product-attributes-item__value">N/A</td></tr>
        <tr><th class="woocommerce-product-attributes-item__label">Gender</th>
            <td class="woocommerce-product-attributes-item__value">N/A</td></tr>
    </table>
</body></html>
"""

MOCK_PRODUCT_3_HTML_NO_INFO = """
<html><body>
    <h1 class="product_title">Product 3</h1>
    <p class="price">$100.00</p>
    <div class="star-rating" aria-label="Rated 5.00 out of 5"></div>
</body></html>
"""


def test_scrape_product_details_success(requests_mock):
    """Test scraping detail produk yang sukses."""
    url = "https://fashion-studio.dicoding.dev/index.php/product/product-1/"
    requests_mock.get(url, text=MOCK_PRODUCT_1_HTML)
    
    data = scrape_product_details(url)
    
    assert data is not None
    assert data['Title'] == 'Product 1'
    assert data['Price'] == '$50.00'
    assert data['Rating'] == '4.50'
    assert data['Colors'] == 'Blue, Red'
    assert data['Size'] == 'M, L'
    assert data['Gender'] == 'Men'

def test_scrape_product_details_invalid(requests_mock):
    """Test scraping produk dengan data invalid."""
    url = "https://fashion-studio.dicoding.dev/index.php/product/product-2/"
    requests_mock.get(url, text=MOCK_PRODUCT_2_HTML_INVALID)
    
    data = scrape_product_details(url)
    
    assert data is not None
    assert data['Title'] == 'Unknown Product'
    assert data['Price'] == 'Price Unavailable'
    assert data['Rating'] == 'Invalid Rating'
    assert data['Colors'] == 'N/A'

def test_scrape_product_details_no_info_table(requests_mock):
    """Test scraping produk tanpa tabel informasi tambahan."""
    url = "https://fashion-studio.dicoding.dev/index.php/product/product-3/"
    requests_mock.get(url, text=MOCK_PRODUCT_3_HTML_NO_INFO)
    
    data = scrape_product_details(url)
    
    assert data is not None
    assert data['Title'] == 'Product 3'
    assert data['Price'] == '$100.00'
    assert data['Rating'] == '5.00'
    assert data['Colors'] == 'N/A' 
    assert data['Size'] == 'N/A' 
    assert data['Gender'] == 'N/A' 

def test_scrape_product_details_request_failure(requests_mock):
    """Test kegagalan request (misal 404)."""
    url = "https://fashion-studio.dicoding.dev/index.php/product/fail/"
    requests_mock.get(url, status_code=404)
    
    data = scrape_product_details(url)
    assert data is None

def test_extract_data(requests_mock):
    """Test fungsi extract_data (integrasi)."""
    base_url = "https://fashion-studio.dicoding.dev"
    shop_url = f"{base_url}/index.php/shop/page/1/"
    prod1_url = "https://fashion-studio.dicoding.dev/index.php/product/product-1/"
    prod2_url = "https://fashion-studio.dicoding.dev/index.php/product/product-2/"
    
    requests_mock.get(shop_url, text=MOCK_SHOP_PAGE_HTML)
    requests_mock.get(f"{base_url}/index.php/shop/page/2/", text="<html></html>")
    
    requests_mock.get(prod1_url, text=MOCK_PRODUCT_1_HTML)
    requests_mock.get(prod2_url, text=MOCK_PRODUCT_2_HTML_INVALID)
    
    df = extract_data(base_url, total_pages=2)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert 'timestamp' in df.columns
    assert df.loc[0, 'Title'] == 'Product 1'
    assert df.loc[1, 'Title'] == 'Unknown Product'