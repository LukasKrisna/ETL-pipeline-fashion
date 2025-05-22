from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
import sqlite3
from datetime import datetime
import os


def create_database(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create products table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                price REAL,
                rating REAL,
                colors TEXT,
                size TEXT,
                gender TEXT,
                timestamp DATETIME,
                transformed_at DATETIME
            )
        ''')

        conn.commit()
        print(f"Database initialized at {db_path}")

    except sqlite3.Error as e:
        raise ValueError(f"Database initialization error: {str(e)}")
    finally:
        if conn:
            conn.close()


def save_to_database(df, db_path):
    if df.empty:
        raise ValueError("No data to save to database")

    try:
        # Ensure database exists
        create_database(db_path)

        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Convert lists to strings for storage
        df['colors'] = df['Colors'].apply(lambda x: ','.join(x) if x else '')
        df['size'] = df['Size'].apply(lambda x: ','.join(x) if x else '')

        # Prepare data for insertion
        records = df[['Title', 'Price', 'Rating', 'colors', 'size', 'Gender', 'timestamp', 'transformed_at']].values.tolist()

        # Insert data
        cursor.executemany('''
            INSERT INTO products (title, price, rating, colors, size, gender, timestamp, transformed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', records)

        conn.commit()
        print(f"Successfully saved {len(records)} records to database")

    except sqlite3.Error as e:
        raise ValueError(f"Database operation error: {str(e)}")
    finally:
        if conn:
            conn.close()


def save_to_csv(df, output_dir):
    if df.empty:
        raise ValueError("No data to save to CSV")

    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'fashion_data_{timestamp}.csv'
        filepath = os.path.join(output_dir, filename)

        # Save to CSV
        df.to_csv(filepath, index=False)
        print(f"Successfully saved data to {filepath}")

    except Exception as e:
        raise ValueError(f"CSV file operation error: {str(e)}")


def load_data(df, db_path, output_dir):
    if df.empty:
        raise ValueError("No data to load")

    try:
        # Save to database
        save_to_database(df, db_path)

        # Save to CSV
        save_to_csv(df, output_dir)

        print("Data loading completed successfully")

    except Exception as e:
        raise ValueError(f"Data loading error: {str(e)}")


def save_to_google_sheets(df, service_account_file, spreadsheet_id, scopes):
    try:
        RANGE_NAME = 'Sheet1!A1'
        credential = Credentials.from_service_account_file(
            service_account_file, scopes=scopes)
        service = build('sheets', 'v4', credentials=credential)
        sheet = service.spreadsheets()

        values = [df.columns.tolist()] + df.values.tolist()

        body = {'values': values}

        result = sheet.values().update(
            spreadsheetId=spreadsheet_id,
            range=RANGE_NAME,
            valueInputOption='RAW',
            body=body
        ).execute()

        print(f"Data saved to Google Sheets: {result}")
        return result

    except Exception as e:
        raise ValueError(f"Error saving to Google Sheets: {str(e)}")


def save_to_postgresql(df, db_url, table_name):
    try:
        engine = create_engine(db_url)

        with engine.connect() as con:
            df.to_sql(table_name, con=con,
                      if_exists='append', index=False)
            print("Data saved to PostgreSQL")

    except Exception as e:
        raise ValueError(f"Error saving to PostgreSQL: {str(e)}")
