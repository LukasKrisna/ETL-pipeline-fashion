import pytest
import requests
from bs4 import BeautifulSoup
from unittest.mock import Mock, patch
from utils.extract import fetch_webpage, parse_product_info, scrape_product

# Test fetch_webpage function
def test_fetch_webpage_success():
    with patch('requests.Session') as mock_session:
        mock_response = Mock()
        mock_response.content = b"<html>Test content</html>"
        mock_session.return_value.get.return_value = mock_response

        result = fetch_webpage("https://test-url.com")
        assert result == b"<html>Test content</html>"


def test_fetch_webpage_failure():
    with patch('requests.Session') as mock_session:
        mock_session.return_value.get.side_effect = requests.exceptions.RequestException

        with pytest.raises(ValueError) as exc_info:
            fetch_webpage("https://test-url.com")
        assert "Failed to fetch" in str(exc_info.value)


def test_fetch_webpage_max_attempts():
    with patch('requests.Session') as mock_session:
        mock_session.return_value.get.side_effect = requests.exceptions.RequestException

        with pytest.raises(ValueError) as exc_info:
            fetch_webpage("https://test-url.com", max_attempts=2)
        assert "Failed to fetch" in str(exc_info.value)
        assert mock_session.return_value.get.call_count == 2

# Test parse_product_info function
def test_parse_product_info_complete():
    html = """
    <div class="collection-card">
        <h3 class="product-title">Test Product</h3>
        <span class="price">$99.99</span>
        <p>Rating: 4.5 / 5</p>
        <p>Colors: Red, Blue, Green</p>
        <p>Size: Large, Medium</p>
        <p>Gender: Unisex</p>
    </div>
    """
    soup = BeautifulSoup(html, 'html.parser')
    card = soup.find('div', class_='collection-card')

    result = parse_product_info(card)

    assert result['Title'] == 'Test Product'
    assert result['Price'] == '$99.99'
    assert result['Rating'] == 'Rating: 4.5 / 5'
    assert result['Colors'] == 'Colors: Red, Blue, Green'
    assert result['Size'] == 'Size: Large, Medium'
    assert result['Gender'] == 'Gender: Unisex'
    assert 'timestamp' in result


def test_parse_product_info_missing_fields():
    html = """
    <div class="collection-card">
        <h3 class="product-title">Test Product</h3>
    </div>
    """
    soup = BeautifulSoup(html, 'html.parser')
    card = soup.find('div', class_='collection-card')

    result = parse_product_info(card)

    assert result['Title'] == 'Test Product'
    assert result['Price'] is None
    assert result['Rating'] is None
    assert result['Colors'] is None
    assert result['Size'] is None
    assert result['Gender'] is None
    assert 'timestamp' in result


def test_parse_product_info_invalid_card():
    with pytest.raises(ValueError) as exc_info:
        parse_product_info(None)
    assert "Error extracting product data" in str(exc_info.value)

# Test scrape_product function
def test_scrape_product_single_page():
    with patch('utils.extract.fetch_webpage') as mock_fetch:
        html_content = """
        <div class="collection-card">
            <h3 class="product-title">Product 1</h3>
            <span class="price">$99.99</span>
        </div>
        <div class="collection-card">
            <h3 class="product-title">Product 2</h3>
            <span class="price">$149.99</span>
        </div>
        <li class="page-item next disabled"></li>
        """
        mock_fetch.return_value = html_content.encode()

        results = scrape_product(max_pages=1)
        assert len(results) == 2
        assert results[0]['Title'] == 'Product 1'
        assert results[1]['Title'] == 'Product 2'


def test_scrape_product_multiple_pages():
    with patch('utils.extract.fetch_webpage') as mock_fetch:
        page1_content = """
        <div class="collection-card">
            <h3 class="product-title">Product 1</h3>
        </div>
        """
        page2_content = """
        <div class="collection-card">
            <h3 class="product-title">Product 2</h3>
        </div>
        <li class="page-item next disabled"></li>
        """
        mock_fetch.side_effect = [page1_content.encode(), page2_content.encode()]

        results = scrape_product(max_pages=2)
        assert len(results) == 2
        assert results[0]['Title'] == 'Product 1'
        assert results[1]['Title'] == 'Product 2'


def test_scrape_product_general_error():
    with patch('utils.extract.fetch_webpage') as mock_fetch:
        mock_fetch.side_effect = Exception('Unexpected error')

        with pytest.raises(ValueError) as exc_info:
            scrape_product(max_pages=1)
        assert "Error during scraping" in str(exc_info.value)


def test_scrape_product_keyboard_interrupt():
    with patch('utils.extract.fetch_webpage') as mock_fetch:
        mock_fetch.side_effect = KeyboardInterrupt()

        with pytest.raises(ValueError) as exc_info:
            scrape_product(max_pages=1)
        assert "Scraping was interrupted by user" in str(exc_info.value)
