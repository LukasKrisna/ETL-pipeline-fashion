import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
}


def fetch_webpage(url, max_attempts=3):
    session = requests.Session()

    for attempt in range(max_attempts):
        try:
            response = session.get(url, headers=REQUEST_HEADERS, timeout=10)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"Failed to fetch {url} (attempt {attempt+1}/{max_attempts}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(2)

    raise ValueError(f"Failed to fetch {url} after {max_attempts} attempts")


def parse_product_info(card):
    try:
        title = card.find('h3', class_='product-title')
        price_element = card.find(['span', 'p'], class_='price')
        rating_element = card.find(
            'p', string=lambda text: text and 'Rating:' in text)
        colors_element = card.find(
            'p', string=lambda text: text and 'Colors' in text)
        size_element = card.find(
            'p', string=lambda text: text and 'Size:' in text)
        gender_element = card.find(
            'p', string=lambda text: text and 'Gender:' in text)

        product_info = {
            'Title': title.text.strip() if title else None,
            'Price': price_element.text.strip() if price_element else None,
            'Rating': rating_element.text.strip() if rating_element else None,
            'Colors': colors_element.text.strip() if colors_element else None,
            'Size': size_element.text.strip() if size_element else None,
            'Gender': gender_element.text.strip() if gender_element else None,
            'timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
        }

        return product_info
    except Exception as e:
        raise ValueError(f"Error extracting product data: {str(e)}")


def scrape_product(start_page=1, delay=1, max_pages=None):
    product_list = []
    current_page = start_page
    pages_processed = 0

    try:
        while True:
            if max_pages and pages_processed >= max_pages:
                break

            url = "https://fashion-studio.dicoding.dev/" if current_page == 1 else f"https://fashion-studio.dicoding.dev/page{current_page}"
            print(f"Processing page {current_page}: {url}")

            content = fetch_webpage(url)
            if not content:
                current_page += 1
                pages_processed += 1
                continue

            soup = BeautifulSoup(content, "html.parser")
            cards = soup.find_all('div', class_='collection-card')

            for card in cards:
                product = parse_product_info(card)
                if product:
                    product_list.append(product)

            next_button = soup.find('li', class_='page-item next disabled')
            if not next_button:
                current_page += 1
                pages_processed += 1
                time.sleep(delay)
            else:
                print("Reached final page")
                break

    except KeyboardInterrupt:
        print("Scraping interrupted by user")
        raise ValueError("Scraping was interrupted by user")
    except Exception as e:
        raise ValueError(f"Error during scraping: {str(e)}")

    print(f"Successfully scraped {len(product_list)} products from {pages_processed+1} pages")
    return product_list
