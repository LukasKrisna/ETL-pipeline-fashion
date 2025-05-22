from utils.extract import scrape_product
from utils.transform import create_dataframe, process_dataframe
from utils.load import save_to_csv, save_to_google_sheets, save_to_postgresql


def main():
    try:
        RATE_CONVERSION = 16000.0
        DB_CONNECTION = 'postgresql+psycopg2://developer:supersecretpassword@localhost:5432/etl_submission_db'
        TARGET_TABLE = "products_data"
        GOOGLE_CREDENTIALS = './google-sheets-api.json'
        SHEET_ID = '1fnPxCovTCKu7L-NgDJcBcMk0Lo8eoWpyW3IVixiqa_g'
        API_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        print("ETL process initiated")

        # Step 1: Extract data
        print("Phase 1: Data extraction in progress")
        scraped_items = scrape_product()
        print(f"Successfully extracted {len(scraped_items)} items")
        initial_data = create_dataframe(scraped_items)
        save_to_csv(initial_data, "raw_data.csv")

        # Step 2: Transform data
        print("Phase 2: Data transformation in progress")
        processed_data = process_dataframe(initial_data, RATE_CONVERSION)
        print(f"Successfully processed {len(processed_data)} records")
        save_to_csv(processed_data, "transformed_data.csv")

        # Step 3: Load data
        print("Phase 3: Data loading in progress")
        save_to_csv(processed_data, "products.csv")
        save_to_postgresql(processed_data, DB_CONNECTION, TARGET_TABLE)
        save_to_google_sheets(
            processed_data, GOOGLE_CREDENTIALS, SHEET_ID, API_SCOPES)

        print("ETL process completed successfully")
        return 0

    except Exception as e:
        raise ValueError(f"ETL process failed: {str(e)}")


if __name__ == "__main__":
    exit(main())
