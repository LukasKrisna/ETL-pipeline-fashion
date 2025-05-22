# ETL Pipeline Fashion Data

## Project Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline for scraping and processing competitor data from a fashion retail website. As a data engineer working for a fashion retail company, this tool allows us to collect and prepare competitive intelligence on Fashion Studio, a major competitor in the fashion and design industry.

## Business Context

Our fashion retail company constantly releases new clothing items each month. However, we're facing increasing competition in the market, with many new competitors emerging in a short time. These competitors are skilled at manipulating prices, promotions, and introducing new clothing models.

To stay competitive, our company's data team has been tasked with researching competitor pricing and products. This project specifically targets Fashion Studio (https://fashion-studio.dicoding.dev), which regularly releases various fashion products including t-shirts, pants, jackets, and outerwear.

## Project Structure

```
├── utils/                      # Core ETL functionality modules
│   ├── extract.py              # Data extraction module (web scraping)
│   ├── transform.py            # Data transformation and cleaning
│   └── load.py                 # Data loading to various destinations
├── tests/                      # Unit tests for the ETL pipeline
│   ├── test_extract.py         # Tests for extraction functionality
│   ├── test_transform.py       # Tests for transformation functionality
│   └── test_load.py            # Tests for loading functionality
├── .env/                       # Virtual environment (not tracked in git)
├── main.py                     # Main ETL pipeline controller
├── products.csv                # Final processed data
├── raw_data.csv                # Raw scraped data
├── transformed_data.csv        # Intermediate transformed data
├── requirements.txt            # Project dependencies
└── README.md                   # Project documentation
```

## ETL Pipeline Components

### 1. Extract (utils/extract.py)

The extraction module handles web scraping of the Fashion Studio website. It includes:

- `fetch_webpage()`: Retrieves HTML content from the target URL with retry logic
- `parse_product_info()`: Parses product information from HTML elements
- `scrape_product()`: Orchestrates the scraping process across multiple pages

### 2. Transform (utils/transform.py)

The transformation module processes and cleans the scraped data. It includes:

- `create_dataframe()`: Converts raw data into a pandas DataFrame
- `process_dataframe()`: Performs data cleaning and standardization:
  - Removes invalid entries and duplicates
  - Standardizes price format and converts currency (using exchange rate)
  - Extracts numeric values from rating and color fields
  - Cleans up text fields by removing prefixes

### 3. Load (utils/load.py)

The loading module saves the processed data to various destinations:

- `save_to_csv()`: Saves the data to CSV files
- `save_to_google_sheets()`: Exports data to Google Sheets
- `save_to_postgresql()`: Stores data in a PostgreSQL database

## How to Use

### Prerequisites

- Python 3.8+
- PostgreSQL database
- Google Sheets API credentials

### Installation

1. Clone this repository
2. Create a virtual environment:
   ```
   python -m venv .env
   ```
3. Activate the virtual environment:

   ```
   # On Windows
   .env\Scripts\activate

   # On macOS/Linux
   source .env/bin/activate
   ```

4. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

### Configuration

1. Set up a PostgreSQL database
2. Generate Google Sheets API credentials and save as `google-sheets-api.json`
3. Update database connection parameters and spreadsheet ID in `main.py` if needed

### Running the ETL Pipeline

Execute the main script to run the complete ETL pipeline:

```
python main.py
```

### Testing

Run the test suite to verify the functionality:

```
python -m pytest tests/
```

Run Coverage Report:

```
coverage run -m pytest tests/
```

See Coverage Report:

```
coverage report -m
```

## Output Files

- `raw_data.csv`: Raw scraped data without processing
- `transformed_data.csv`: Intermediate data after transformation
- `products.csv`: Final cleaned data ready for analysis
