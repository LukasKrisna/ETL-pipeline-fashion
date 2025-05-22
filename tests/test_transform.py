import pytest
import pandas as pd
import numpy as np
from utils.transform import (
    create_dataframe,
    process_dataframe,
    clean_price,
    clean_rating,
    extract_colors,
    extract_sizes,
    extract_gender,
    transform_data
)

# Test create_dataframe function
def test_create_dataframe_dict():
    data = {
        'Title': ['Product 1', 'Product 2'],
        'Price': ['$99.99', '$149.99']
    }
    df = create_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)
    assert list(df.columns) == ['Title', 'Price']


def test_create_dataframe_list():
    data = [
        {'Title': 'Product 1', 'Price': '$99.99'},
        {'Title': 'Product 2', 'Price': '$149.99'}
    ]
    df = create_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)
    assert list(df.columns) == ['Title', 'Price']


def test_create_dataframe_none():
    with pytest.raises(ValueError) as exc_info:
        create_dataframe(None)
    assert "Input data cannot be None" in str(exc_info.value)


def test_create_dataframe_invalid():
    with pytest.raises(ValueError) as exc_info:
        create_dataframe("invalid data")
    assert "Error converting data to DataFrame" in str(exc_info.value)


def test_process_dataframe_invalid_input():
    with pytest.raises(ValueError) as exc_info:
        process_dataframe("not a dataframe", 16000)
    assert "Input data must be a pandas DataFrame" in str(exc_info.value)


def test_process_dataframe_invalid_exchange_rate():
    data = pd.DataFrame({
        'Title': ['Product 1'],
        'Price': ['$99.99']
    })

    with pytest.raises(ValueError):
        process_dataframe(data, conversion_rate=-1)

    with pytest.raises(ValueError):
        process_dataframe(data, conversion_rate="invalid")


def test_process_dataframe_with_dirty_patterns():
    data = pd.DataFrame({
        'Title': ['Product 1', 'Unknown Product', 'Product 2'],
        'Price': ['$99.99', 'Price Unavailable', '$149.99'],
        'Rating': ['4.5 / 5', 'Invalid Rating / 5', '3.8 / 5'],
        'Colors': ['3', '2', '1'], 
        'Size': ['Size: Large', 'Size: Medium', 'Size: Small'],
        'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex']
    })
    
    result = process_dataframe(data, 16000)
    assert len(result) == 2  
    assert result['Title'].iloc[0] == 'Product 1'
    assert result['Title'].iloc[1] == 'Product 2'
    assert result['Price'].iloc[0] == 99.99 * 16000
    assert result['Price'].iloc[1] == 149.99 * 16000


def test_process_dataframe_with_missing_values():
    data = pd.DataFrame({
        'Title': ['Product 1', None, 'Product 2'],
        'Price': ['$99.99', '$149.99', None],
        'Rating': ['4.5 / 5', None, '3.8 / 5'],
        'Colors': ['3', '2', None],  
        'Size': ['Size: Large', None, 'Size: Small'],
        'Gender': ['Gender: Men', 'Gender: Women', None]
    })
    
    result = process_dataframe(data, 16000)
    assert len(result) == 1 
    assert result['Title'].iloc[0] == 'Product 1'
    assert result['Price'].iloc[0] == 99.99 * 16000


# Test clean_price function
def test_clean_price_valid():
    assert clean_price('$99.99') == 99.99
    assert clean_price('1,234.56') == 1234.56
    assert clean_price('0.99') == 0.99


def test_clean_price_invalid():
    assert clean_price(None) is None
    assert clean_price('') is None
    assert clean_price('invalid') is None
    assert clean_price('$invalid') is None


# Test clean_rating function
def test_clean_rating_valid():
    assert clean_rating('Rating: 4.5 / 5') == 4.5
    assert clean_rating('3.8') == 3.8
    assert clean_rating('5.0') == 5.0
    assert clean_rating('Rating: 4.5') == 4.5


def test_clean_rating_invalid():
    assert clean_rating(None) is None
    assert clean_rating('') is None
    assert clean_rating('invalid') is None
    assert clean_rating('Rating: invalid') is None


# Test extract_colors function
def test_extract_colors_valid():
    assert extract_colors('Colors: Red, Blue, Green') == ['Red', 'Blue', 'Green']
    assert extract_colors('Colors: Black') == ['Black']
    assert extract_colors('Colors: ') == ['']


def test_extract_colors_invalid():
    assert extract_colors(None) == []
    assert extract_colors('') == []
    assert extract_colors('No colors here') == ['No colors here']  


# Test extract_sizes function
def test_extract_sizes_valid():
    assert extract_sizes('Size: Large, Medium') == ['Large', 'Medium']
    assert extract_sizes('Size: Small') == ['Small']
    assert extract_sizes('Size: ') == ['']


def test_extract_sizes_invalid():
    assert extract_sizes(None) == []
    assert extract_sizes('') == []
    assert extract_sizes('No size here') == ['No size here']  


# Test extract_gender function
def test_extract_gender_valid():
    assert extract_gender('Gender: Unisex') == 'Unisex'
    assert extract_gender('Gender: Men') == 'Men'
    assert extract_gender('Gender: Women') == 'Women'


def test_extract_gender_invalid():
    assert extract_gender(None) is None
    assert extract_gender('') is None
    assert extract_gender('No gender here') == 'No gender here' 


# Test transform_data function
def test_transform_data_valid():
    data = [
        {
            'Title': 'Product 1',
            'Price': '$99.99',
            'Rating': 'Rating: 4.5 / 5',
            'Colors': 'Colors: Red, Blue',
            'Size': 'Size: Large',
            'Gender': 'Gender: Unisex',
            'timestamp': '2024-01-01T00:00:00.000000'
        }
    ]

    result = transform_data(data)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result['Title'].iloc[0] == 'Product 1'
    assert result['Price'].iloc[0] == 99.99
    assert result['Rating'].iloc[0] == 4.5
    assert result['Colors'].iloc[0] == ['Red', 'Blue']
    assert result['Size'].iloc[0] == ['Large']
    assert result['Gender'].iloc[0] == 'Unisex'
    assert 'transformed_at' in result.columns


def test_transform_data_empty():
    with pytest.raises(ValueError) as exc_info:
        transform_data([])
    assert "No data provided for transformation" in str(exc_info.value)


def test_transform_data_invalid():
    with pytest.raises(ValueError) as exc_info:
        transform_data(None)
    assert "No data provided for transformation" in str(exc_info.value)


def test_transform_data_with_invalid_timestamp():
    data = [
        {
            'Title': 'Product 1',
            'Price': '$99.99',
            'Rating': 'Rating: 4.5 / 5',
            'Colors': 'Colors: Red, Blue',
            'Size': 'Size: Large',
            'Gender': 'Gender: Unisex',
            'timestamp': 'invalid-timestamp'
        }
    ]

    with pytest.raises(ValueError) as exc_info:
        transform_data(data)
    assert "Error during data transformation" in str(exc_info.value)
