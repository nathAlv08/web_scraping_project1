import pytest
import pandas as pd
import numpy as np
from utils.transform import transform_data
from datetime import datetime

@pytest.fixture
def raw_data():
    """Fixture untuk menyediakan data mentah sample."""
    return pd.DataFrame({
        'Title': ['Test Shirt 1', 'Unknown Product', 'Test Shirt 2', 'Test Shirt 1', 'Test Jacket'],
        'Price': ['$50.00', 'Price Unavailable', '$1,000.00', '$50.00', '$100.00'],
        'Rating': ['4.8', 'Invalid Rating', '3.2', '4.8', '4.0'],
        'Colors': ['Blue, Red', 'N/A', 'Green', 'Blue, Red', 'Black'],
        'Size': ['Size: M', 'N/A', 'Size: L', 'Size: M', 'N/A'],
        'Gender': ['Gender: Men', 'N/A', 'Gender: Women', 'Gender: Men', 'Gender: Men'],
        'timestamp': [datetime.now()] * 5
    })

def test_transform_data_success(raw_data):
    """Test transformasi data yang sukses."""
    cleaned_df = transform_data(raw_data, exchange_rate=16000)
    
    assert len(cleaned_df) == 2 

    assert cleaned_df.iloc[0]['Title'] == 'Test Shirt 1'
    assert cleaned_df.iloc[0]['Price (IDR)'] == 800000 
    assert cleaned_df.iloc[0]['Rating'] == pytest.approx(4.8)
    assert cleaned_df.iloc[0]['Colors'] == 2
    assert cleaned_df.iloc[0]['Size'] == 'M'
    assert cleaned_df.iloc[0]['Gender'] == 'Men'
    
    assert cleaned_df.iloc[1]['Title'] == 'Test Shirt 2'
    assert cleaned_df.iloc[1]['Price (IDR)'] == 16000000
    assert cleaned_df.iloc[1]['Rating'] == pytest.approx(3.2)
    assert cleaned_df.iloc[1]['Colors'] == 1 
    assert cleaned_df.iloc[1]['Size'] == 'L'
    assert cleaned_df.iloc[1]['Gender'] == 'Women'

    assert pd.api.types.is_string_dtype(cleaned_df['Title'])
    assert pd.api.types.is_integer_dtype(cleaned_df['Price (IDR)'])
    assert pd.api.types.is_float_dtype(cleaned_df['Rating'])
    assert pd.api.types.is_integer_dtype(cleaned_df['Colors'])
    assert pd.api.types.is_string_dtype(cleaned_df['Size'])
    assert pd.api.types.is_string_dtype(cleaned_df['Gender'])
    assert pd.api.types.is_datetime64_any_dtype(cleaned_df['timestamp'])

def test_transform_data_all_invalid():
    """Test jika semua data invalid dan di-drop."""
    df = pd.DataFrame({
        'Title': ['Unknown Product', 'Test Shirt'],
        'Price': ['Price Unavailable', '$10'],
        'Rating': ['Invalid Rating', '4.0'],
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