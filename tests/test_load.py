import pytest
import pandas as pd
import os
import sqlite3
from unittest.mock import Mock, patch, MagicMock
from utils.load import (
    create_database,
    save_to_database,
    save_to_google_sheets,
    save_to_postgresql,
    load_data
)

# Test data setup
@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        'Title': ['Product 1', 'Product 2'],
        'Price': [99.99, 149.99],
        'Rating': [4.5, 3.8],
        'Colors': [['Red', 'Blue'], ['Green']],
        'Size': [['Large'], ['Medium']],
        'Gender': ['Unisex', 'Women'],
        'timestamp': ['2024-01-01T00:00:00.000000', '2024-01-01T00:00:00.000000'],
        'transformed_at': ['2024-01-01T00:00:00.000000', '2024-01-01T00:00:00.000000']
    })

# Test create_database function
def test_create_database_success(tmp_path):
    db_path = os.path.join(tmp_path, "test.db")
    create_database(db_path)
    
    assert os.path.exists(db_path)
    
    # Verify table structure
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products'")
    assert cursor.fetchone() is not None
    
    cursor.execute("PRAGMA table_info(products)")
    columns = {row[1] for row in cursor.fetchall()}
    expected_columns = {
        'id', 'title', 'price', 'rating', 'colors', 'size', 
        'gender', 'timestamp', 'transformed_at'
    }
    assert columns == expected_columns
    conn.close()


# Test save_to_database function
def test_save_to_database_success(sample_dataframe, tmp_path):
    db_path = os.path.join(tmp_path, "test.db")
    create_database(db_path)
    save_to_database(sample_dataframe, db_path)
    
    # Verify data was saved
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    assert cursor.fetchone()[0] == 2
    
    cursor.execute("SELECT title, price, rating FROM products")
    rows = cursor.fetchall()
    assert rows[0] == ('Product 1', 99.99, 4.5)
    assert rows[1] == ('Product 2', 149.99, 3.8)
    conn.close()


def test_save_to_database_empty_dataframe(tmp_path):
    db_path = os.path.join(tmp_path, "test.db")
    create_database(db_path)
    
    with pytest.raises(ValueError) as exc_info:
        save_to_database(pd.DataFrame(), db_path)
    assert "No data to save to database" in str(exc_info.value)


# Test save_to_google_sheets function
def test_save_to_google_sheets_success(sample_dataframe):
    mock_service = Mock()
    mock_sheet = Mock()
    mock_values = Mock()

    mock_service.spreadsheets.return_value = mock_sheet
    mock_sheet.values.return_value = mock_values
    mock_values.update.return_value.execute.return_value = {'updatedRows': 3}

    with patch('utils.load.Credentials') as mock_creds, \
            patch('utils.load.build') as mock_build:
        mock_build.return_value = mock_service

        result = save_to_google_sheets(
            sample_dataframe,
            'fake_service_account.json',
            'spreadsheet_id',
            ['https://www.googleapis.com/auth/spreadsheets']
        )

        assert result == {'updatedRows': 3}
        mock_build.assert_called_once_with(
            'sheets', 'v4', credentials=mock_creds.from_service_account_file.return_value)


def test_save_to_google_sheets_invalid_credentials(sample_dataframe):
    with patch('utils.load.Credentials') as mock_creds:
        mock_creds.from_service_account_file.side_effect = Exception('Invalid credentials')

        with pytest.raises(ValueError) as exc_info:
            save_to_google_sheets(
                sample_dataframe,
                'invalid_service_account.json',
                'spreadsheet_id',
                ['https://www.googleapis.com/auth/spreadsheets']
            )
        assert "Error saving to Google Sheets" in str(exc_info.value)

# Test save_to_postgresql function
def test_save_to_postgresql_success(sample_dataframe):
    mock_engine = Mock()
    mock_connection = MagicMock()
    mock_engine.connect.return_value = mock_connection

    with patch('utils.load.create_engine') as mock_create_engine:
        mock_create_engine.return_value = mock_engine

        save_to_postgresql(
            sample_dataframe,
            'postgresql://user:pass@localhost:5432/db',
            'test_table'
        )

        mock_create_engine.assert_called_once_with(
            'postgresql://user:pass@localhost:5432/db')
        assert mock_engine.connect.called
        assert mock_connection.__enter__.called
        assert mock_connection.__exit__.called


def test_save_to_postgresql_connection_error(sample_dataframe):
    with patch('utils.load.create_engine') as mock_create_engine:
        mock_create_engine.side_effect = Exception('Connection failed')

        with pytest.raises(ValueError) as exc_info:
            save_to_postgresql(
                sample_dataframe,
                'postgresql://invalid:invalid@localhost:5432/db',
                'test_table'
            )
        assert "Error saving to PostgreSQL" in str(exc_info.value)

# Test load_data function
def test_load_data_success(sample_dataframe, tmp_path):
    db_path = os.path.join(tmp_path, "test.db")
    output_dir = str(tmp_path)
    
    load_data(sample_dataframe, db_path, output_dir)
    
    # Verify database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM products")
    assert cursor.fetchone()[0] == 2
    conn.close()
    
    # Verify CSV
    csv_files = [f for f in os.listdir(output_dir) if f.startswith('fashion_data_')]
    assert len(csv_files) == 1


def test_load_data_empty_dataframe(tmp_path):
    with pytest.raises(ValueError) as exc_info:
        load_data(pd.DataFrame(), "test.db", str(tmp_path))
    assert "No data to load" in str(exc_info.value)
