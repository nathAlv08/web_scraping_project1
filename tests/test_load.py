import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from utils.load import load_to_csv, load_to_gdrive, load_to_postgres
from sqlalchemy.exc import SQLAlchemyError
from googleapiclient.errors import HttpError
import os

@pytest.fixture
def cleaned_data():
    """Fixture untuk menyediakan data bersih sample."""
    return pd.DataFrame({
        'title': ['Test Shirt 1'],
        'Price': [800000.0],
        'Rating': [4.8],
        'colors': [2],
        'size': ['M'],
        'gender': ['Men'],
        'timestamp': [pd.to_datetime('2024-01-01')]
    })

@patch('pandas.DataFrame.to_csv')
def test_load_to_csv_success(mock_to_csv, cleaned_data):
    """Test load ke CSV sukses."""
    file_path = "test_products.csv"
    load_to_csv(cleaned_data, file_path)
    
    mock_to_csv.assert_called_once_with(file_path, index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')

@patch('pandas.DataFrame.to_csv', side_effect=IOError("Permission denied"))
def test_load_to_csv_failure(mock_to_csv, cleaned_data, caplog):
    """Test load ke CSV gagal (IOError)."""
    file_path = "permission_denied.csv"
    load_to_csv(cleaned_data, file_path)
    assert "Gagal menyimpan ke CSV" in caplog.text

@patch('utils.load.build')
@patch('utils.load.service_account.Credentials.from_service_account_file')
@patch('os.path.exists', return_value=True)
def test_load_to_gdrive_success(mock_exists, mock_creds, mock_build, cleaned_data):
    """Test load ke Google Sheets sukses."""
    mock_service = MagicMock()
    mock_build.return_value = mock_service
    
    sheet_id = "test_sheet_id"
    creds_path = "fake_creds.json"
    
    load_to_gdrive(cleaned_data, sheet_id, creds_path)
    
    mock_service.spreadsheets().values().clear.assert_called_once()
    mock_service.spreadsheets().values().update.assert_called_once()
    
    args, kwargs = mock_service.spreadsheets().values().update.call_args
    sent_body = kwargs['body']
    
    expected_values = [
        ['title', 'Price', 'Rating', 'colors', 'size', 'gender', 'timestamp'],
        ['Test Shirt 1', 800000.0, 4.8, 2, 'M', 'Men', '2024-01-01 00:00:00'] 
    ]
    assert sent_body['values'] == expected_values

@patch('utils.load.build', side_effect=HttpError(MagicMock(status=403), b"Permission denied"))
@patch('utils.load.service_account.Credentials.from_service_account_file')
@patch('os.path.exists', return_value=True)
def test_load_to_gdrive_failure(mock_exists, mock_creds, mock_build, cleaned_data, caplog):
    """Test load ke Google Sheets gagal (HttpError)."""
    load_to_gdrive(cleaned_data, "test_sheet_id", "fake_creds.json")
    assert "Error saat API Google Sheets" in caplog.text

def test_load_to_gdrive_no_creds_file(cleaned_data, caplog):
    """Test jika file kredensial tidak ditemukan."""
    load_to_gdrive(cleaned_data, "test_sheet_id", "non_existent.json")
    assert "File kredensial Google Sheets tidak ditemukan" in caplog.text


@patch('utils.load.create_engine')
@patch('pandas.DataFrame.to_sql')
def test_load_to_postgres_success(mock_to_sql, mock_create_engine, cleaned_data):
    """Test load ke PostgreSQL sukses."""
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    
    db_url = "postgresql://test:test@localhost/testdb"
    table_name = "test_table"
    
    load_to_postgres(cleaned_data, db_url, table_name)
    
    mock_create_engine.assert_called_once_with(db_url)
    mock_to_sql.assert_called_once_with(table_name, mock_engine.connect().__enter__(), if_exists='replace', index=False)

@patch('utils.load.create_engine', side_effect=SQLAlchemyError("Connection failed"))
def test_load_to_postgres_failure(mock_create_engine, cleaned_data, caplog):
    """Test load ke PostgreSQL gagal (SQLAlchemyError)."""
    load_to_postgres(cleaned_data, "postgresql://bad:url@localhost/testdb", "test_table")
    assert "Gagal menyimpan ke PostgreSQL" in caplog.text