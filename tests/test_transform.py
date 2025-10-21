import pytest
import pandas as pd
import numpy as np
from utils.transform import transform_data
from datetime import datetime

@pytest.fixture
def raw_data():
    """Fixture untuk menyediakan data mentah sample."""
    return pd.DataFrame({
        'Title': ['T-shirt 2', 'Unknown Product', 'Hoodie 3', 'T-shirt 2', 'Jacket 5'],
        'Price': ['$102.15', 'Price Unavailable', '$496.88', '$102.15', '$100.00'],
        'Rating': ['Rating: ⭐ 3.9 / 5', 'Rating: ⭐ Invalid / 5', 'Rating: ⭐ 4.8 / 5', 'Rating: ⭐ 3.9 / 5', 'Rating: ⭐ 4.0 / 5'],
        'Colors': ['3 Colors', 'N/A', '3 Colors', '3 Colors', '1 Colors'],
        'Size': ['Size: M,', 'N/A', 'Size: L,', 'Size: M,', 'N/A'], 
        'Gender': ['Gender: Women,', 'N/A', 'Gender: Unisex,', 'Gender: Women,', 'Gender: Men,'],
        'timestamp': [datetime.now()] * 5
    })

def test_transform_data_success(raw_data):
    """Test transformasi data (REVISI FINAL - Sesuai ekspektasi reviewer)."""
    cleaned_df = transform_data(raw_data, exchange_rate=16000)
    
    assert len(cleaned_df) == 2

    expected_headers = ['title', 'Price', 'Rating', 'colors', 'size', 'gender', 'timestamp']
    assert list(cleaned_df.columns) == expected_headers

    row1 = cleaned_df.iloc[0]
    assert row1['title'] == 'T-shirt 2'
    assert row1['Price'] == pytest.approx(1634400.0)
    assert row1['Rating'] == 3.9

    row2 = cleaned_df.iloc[1]
    assert row2['title'] == 'Hoodie 3'
    assert row2['Price'] == pytest.approx(7950080.0)
    assert row2['Rating'] == 4.8

    assert pd.api.types.is_string_dtype(cleaned_df['title'])
    assert pd.api.types.is_float_dtype(cleaned_df['Price'])
    assert pd.api.types.is_float_dtype(cleaned_df['Rating'])
    assert pd.api.types.is_integer_dtype(cleaned_df['colors'])
    assert pd.api.types.is_string_dtype(cleaned_df['size'])
    assert pd.api.types.is_string_dtype(cleaned_df['gender'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df['timestamp'])

def test_transform_data_all_invalid():
    """Test jika semua data invalid dan di-drop."""
    df = pd.DataFrame({
        'Title': ['Unknown Product', 'T-shirt 5'],
        'Price': ['Price Unavailable', '$10'],
        'Rating': ['Rating: ⭐ Invalid / 5', '4.0'],
        'Colors': ['N/A', 'N/A'],
        'Size': ['N/A', 'N/A'], 
        'Gender': ['N/A', 'N/A'],
        'timestamp': [datetime.now()] * 2
    })
    
    cleaned_df = transform_data(df)
    assert cleaned_df.empty

def test_transform_empty_dataframe():
    """Test jika input DataFrame kosong."""
    empty_df = pd.DataFrame(columns=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'timestamp'])
    cleaned_df = transform_data(empty_df)
    assert cleaned_df.empty