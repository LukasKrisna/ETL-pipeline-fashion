import pandas as pd
import re
from datetime import datetime


def create_dataframe(input_data):
    try:
        if input_data is None:
            raise ValueError("Input data cannot be None")

        df = pd.DataFrame(input_data)
        return df

    except (TypeError, ValueError) as e:
        raise ValueError(f"Error converting data to DataFrame: {str(e)}")


def process_dataframe(df, conversion_rate):
    try:
        # Input validation
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input data must be a pandas DataFrame")

        if not isinstance(conversion_rate, (int, float)):
            raise ValueError(
                f"Exchange rate must be a number, got {type(conversion_rate).__name__}")

        if conversion_rate <= 0:
            raise ValueError(
                f"Exchange rate must be positive, got {conversion_rate}")

        # Create a copy to avoid modifying the original DataFrame
        processed_df = df.copy()

        # Define dirty patterns
        invalid_patterns = {
            "Title": ["Unknown Product"],
            "Rating": ["Invalid Rating / 5", "Not Rated"],
            "Price": ["Price Unavailable", None]
        }

        # Drop rows with missing values and duplicates
        try:
            processed_df.dropna(inplace=True)
            processed_df.drop_duplicates(inplace=True)
        except Exception as e:
            raise ValueError(f"Failed to drop rows with missing values: {str(e)}")

        # Remove rows with dirty patterns
        for col, invalid_values in invalid_patterns.items():
            if col in processed_df.columns:
                processed_df = processed_df[~processed_df[col].isin(invalid_values)]

        # Transform Price column
        try:
            processed_df["Price"] = processed_df["Price"].str.replace(
                r"[$,]", "", regex=True).astype(float).mul(conversion_rate).round(2)
        except Exception as e:
            raise ValueError(
                f"Failed to process Price column - check for missing or invalid values: {str(e)}")

        # Transform Rating column
        try:
            processed_df["Rating"] = processed_df["Rating"].str.extract(
                r"([\d.]+)").astype(float)
        except Exception as e:
            raise ValueError(
                f"Failed to process Rating column - check for missing or invalid formats: {str(e)}")

        # Transform Colors column
        try:
            processed_df["Colors"] = processed_df["Colors"].str.extract(
                r"(\d+)").astype(int)
        except Exception as e:
            raise ValueError(
                f"Failed to process Colors column - check for missing or invalid formats: {str(e)}")

        # Transform Size and Gender columns
        try:
            processed_df["Size"] = processed_df["Size"].str.replace(
                "Size: ", "", regex=False)
            processed_df["Gender"] = processed_df["Gender"].str.replace(
                "Gender: ", "", regex=False)
        except Exception as e:
            raise ValueError(
                f"Failed to process Size or Gender columns - check for missing values: {str(e)}")

        # Reset index
        processed_df.reset_index(drop=True, inplace=True)

        return processed_df

    except (ValueError, TypeError) as e:
        raise ValueError(f"Data transformation failed: {str(e)}")
    except Exception as e:
        raise ValueError(f"Unexpected error in transform_data: {str(e)}")


def clean_price(price_str):
    if not price_str:
        return None

    try:
        # Remove currency symbols and convert to float
        price = re.sub(r'[^\d.]', '', price_str)
        return float(price)
    except (ValueError, TypeError):
        return None


def clean_rating(rating_str):
    if not rating_str:
        return None

    try:
        # Extract rating value using regex
        match = re.search(r'(\d+(?:\.\d+)?)', rating_str)
        if match:
            return float(match.group(1))
        return None
    except (ValueError, TypeError):
        return None


def extract_colors(color_str):
    if not color_str:
        return []

    try:
        # Extract colors after "Colors:"
        colors = color_str.split('Colors:')[-1].strip()
        return [color.strip() for color in colors.split(',')]
    except (AttributeError, IndexError):
        return []


def extract_sizes(size_str):
    if not size_str:
        return []

    try:
        # Extract sizes after "Size:"
        sizes = size_str.split('Size:')[-1].strip()
        return [size.strip() for size in sizes.split(',')]
    except (AttributeError, IndexError):
        return []


def extract_gender(gender_str):
    if not gender_str:
        return None

    try:
        # Extract gender after "Gender:"
        gender = gender_str.split('Gender:')[-1].strip()
        return gender
    except (AttributeError, IndexError):
        return None


def transform_data(data):
    if not data:
        raise ValueError("No data provided for transformation")

    try:
        # Create DataFrame from raw data
        df = pd.DataFrame(data)

        # Apply transformations
        df['Price'] = df['Price'].apply(clean_price)
        df['Rating'] = df['Rating'].apply(clean_rating)
        df['Colors'] = df['Colors'].apply(extract_colors)
        df['Size'] = df['Size'].apply(extract_sizes)
        df['Gender'] = df['Gender'].apply(extract_gender)

        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Add transformation timestamp
        df['transformed_at'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')

        print(f"Successfully transformed {len(df)} records")
        return df

    except Exception as e:
        raise ValueError(f"Error during data transformation: {str(e)}")
