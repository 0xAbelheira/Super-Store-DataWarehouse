# Connect to the MySQL database using .env file
import os
from datetime import datetime

import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from mysql.connector import Error

# Load environment variables from .env file
load_dotenv()

def connect():
    try:
        connection_db = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )

        if connection_db.is_connected():
            db_info = connection_db.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            cursor = connection_db.cursor()
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            print(f"Connected to database: {record[0]}")

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    
    return connection_db

def load_data():
    # Read the CSV file
    df = pd.read_csv("Sample - Superstore.csv", encoding="windows-1252")

    # Display the first few rows and column names
    print("Column names:", df.columns.tolist())
    
    return df

def preprocess_merge_duplicate_products(df):
    """
    Preprocess the dataset to merge rows that have the same product in the same order.

    Args:
        df: The original dataframe containing Superstore data

    Returns:
        Preprocessed dataframe with merged duplicate product entries
    """
    print(f"Original dataset shape: {df.shape}")

    # Create a copy to avoid modifying the original dataframe
    processed_df = df.copy()

    # Identify the columns we need to group by to find duplicate order-product combinations
    # These are the columns that should uniquely identify an order item
    group_cols = ["Order ID", "Product ID"]

    # Count how many times each order-product combination appears
    combination_counts = (
        processed_df.groupby(group_cols).size().reset_index(name="count")
    )
    duplicates = combination_counts[combination_counts["count"] > 1]

    print(
        f"Found {len(duplicates)} order-product combinations that appear multiple times"
    )

    if len(duplicates) > 0:
        # Create a new dataframe to hold our merged results
        merged_rows = []

        # Process each group of duplicate order-product combinations
        for _, row in duplicates.iterrows():
            order_id = row["Order ID"]
            product_id = row["Product ID"]

            # Get all rows for this order-product combination
            filter_condition = (processed_df["Order ID"] == order_id) & (
                processed_df["Product ID"] == product_id
            )
            duplicate_rows = processed_df[filter_condition]

            # Take the first row as our template
            merged_row = duplicate_rows.iloc[0].copy()

            # Calculate aggregated values
            total_quantity = duplicate_rows["Quantity"].sum()
            total_sales = duplicate_rows["Sales"].sum()

            # Calculate weighted discount
            # Weight each discount by its proportion of the total quantity
            weighted_discount = (
                duplicate_rows["Discount"] * duplicate_rows["Quantity"] / total_quantity
            ).sum()

            # Calculate profit
            total_profit = duplicate_rows["Profit"].sum()

            # Update the values in our merged row
            merged_row["Quantity"] = total_quantity
            merged_row["Sales"] = total_sales
            merged_row["Discount"] = weighted_discount
            merged_row["Profit"] = total_profit

            merged_rows.append(merged_row)

            # Remove the duplicate rows from our processed dataframe
            processed_df = processed_df[~filter_condition]

        # Add the merged rows back to the dataframe
        merged_df = pd.DataFrame(merged_rows)
        processed_df = pd.concat([processed_df, merged_df], ignore_index=True)

        print(f"After merging duplicates, dataset shape: {processed_df.shape}")

    return processed_df


def create_level_mappings(df):
    """Create mappings for level keys"""
    # Create sub-category ID mapping
    sub_categories = df["Sub-Category"].drop_duplicates().reset_index(drop=True)
    sub_category_mapping = {
        sub_cat: idx + 1 for idx, sub_cat in enumerate(sub_categories)
    }

    # Create country ID mapping
    countries = df["Country"].drop_duplicates().reset_index(drop=True)
    country_mapping = {country: idx + 1 for idx, country in enumerate(countries)}

    # Create city ID mapping
    city_states = df[["City", "State"]].drop_duplicates().reset_index(drop=True)
    city_mapping = {}
    for idx, (_, row) in enumerate(city_states.iterrows(), 1):
        city_mapping[(row["City"], row["State"])] = idx

    return {
        "sub_category": sub_category_mapping,
        "country": country_mapping,
        "city": city_mapping,
    }


# Function to load data into Calendar dimension table
def load_calendar_dimension(connection, df):
    # Extract unique dates from Order Date and Ship Date
    order_dates = pd.to_datetime(df["Order Date"]).dt.date.unique()
    ship_dates = pd.to_datetime(df["Ship Date"]).dt.date.unique()
    all_dates = sorted(set(order_dates) | set(ship_dates))

    # Create year level mapping (sequential IDs for each year)
    years = sorted(set([d.year for d in all_dates]))
    year_mapping = {year: idx for idx, year in enumerate(years, 1)}

    # Create calendar dataframe
    calendar_data = []
    for date in all_dates:
        dt = datetime.combine(date, datetime.min.time())
        calendar_data.append(
            {
                "full_date": date,
                "year_id": year_mapping[dt.year],
                "year_number": dt.year,
                "month_number": dt.month,
                "month_name": dt.strftime("%B"),
                "day_id": dt.day,
                "day_number": dt.day,
            }
        )

    calendar_df = pd.DataFrame(calendar_data)

    # First, populate CalendarMonth table with unique year-month combinations
    month_data = calendar_df[
        ["year_id", "year_number", "month_number", "month_name"]
    ].drop_duplicates()

    cursor = connection.cursor()
    for _, row in month_data.iterrows():
        query = """
        INSERT INTO CalendarMonth (calendar_month_number, calendar_month_name, year_id, year_number)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(
            query,
            (
                row["month_number"],
                row["month_name"],
                row["year_id"],
                row["year_number"],
            ),
        )

    connection.commit()
    print(f"Loaded {len(month_data)} records into CalendarMonth dimension")

    # Get month IDs from CalendarMonth table to use in Calendar table
    cursor.execute(
        """
        SELECT calendar_month_id, year_id, calendar_month_number
        FROM CalendarMonth
    """
    )
    month_mapping = {(row[1], row[2]): row[0] for row in cursor.fetchall()}

    # Now insert data into Calendar table with proper month_id references
    for _, row in calendar_df.iterrows():
        # Get the calendar_month_id using year_id and month_number
        calendar_month_id = month_mapping[(row["year_id"], row["month_number"])]

        query = """
        INSERT INTO Calendar (full_date, year_id, year_number, month_id, month_number, month_name, day_id, day_number)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            query,
            (
                row["full_date"],
                row["year_id"],
                row["year_number"],
                calendar_month_id,  # Use calendar_month_id from CalendarMonth
                row["month_number"],
                row["month_name"],
                row["day_id"],
                row["day_number"],
            ),
        )

    connection.commit()
    print(f"Loaded {len(calendar_df)} records into Calendar dimension")

    # Return the year mapping in case needed elsewhere
    return year_mapping


# Function to load Customer dimension table
def load_customer_dimension(connection, df):
    # Extract unique customer data
    customer_df = df[["Customer ID", "Customer Name", "Segment"]].drop_duplicates()

    # Insert data into Customer table
    cursor = connection.cursor()
    for _, row in customer_df.iterrows():
        query = """
        INSERT INTO Customer (customer_code, customer_name, segment)
        VALUES (%s, %s, %s)
        """
        cursor.execute(
            query, (row["Customer ID"], row["Customer Name"], row["Segment"])
        )

    connection.commit()
    print(f"Loaded {len(customer_df)} records into Customer dimension")


# Function to load Region, State, and Location dimension tables
def load_geography_dimensions(connection, df, level_mappings):
    # Extract unique regions and countries
    region_df = df[["Region", "Country"]].drop_duplicates()

    # Insert data into Region table
    cursor = connection.cursor()
    for _, row in region_df.iterrows():
        country_id = level_mappings["country"][row["Country"]]
        query = """
        INSERT INTO Region (region_name, country_id, country_name)
        VALUES (%s, %s, %s)
        """
        cursor.execute(query, (row["Region"], country_id, row["Country"]))

    connection.commit()
    print(f"Loaded {len(region_df)} records into Region dimension")

    # Extract unique state-region combinations
    state_df = df[["State", "Region", "Country"]].drop_duplicates()

    # Get region IDs
    cursor.execute("SELECT region_id, region_name FROM Region")
    region_mapping = {row[1]: row[0] for row in cursor.fetchall()}

    # Insert data into State table
    for _, row in state_df.iterrows():
        region_id = region_mapping.get(row["Region"])
        country_id = level_mappings["country"][row["Country"]]

        query = """
        INSERT INTO State (state_name, region_id, region_name, country_id, country_name)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(
            query, (row["State"], region_id, row["Region"], country_id, row["Country"])
        )

    connection.commit()
    print(f"Loaded {len(state_df)} records into State dimension")

    # Now load Location table
    location_df = df[
        ["Postal Code", "City", "State", "Country", "Region"]
    ].drop_duplicates()

    # Get state IDs
    cursor.execute("SELECT state_id, state_name FROM State")
    state_mapping = {row[1]: row[0] for row in cursor.fetchall()}

    # Insert data into Location table
    for _, row in location_df.iterrows():
        country_id = level_mappings["country"][row["Country"]]
        state_id = state_mapping.get(row["State"])
        city_id = level_mappings["city"][(row["City"], row["State"])]
        region_id = region_mapping.get(row["Region"])

        query = """
        INSERT INTO Location (location_code, country_id, country_name, state_id, state_name, city_id, city_name, postal_code, region_id, region_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            query,
            (
                row["Postal Code"],
                country_id,
                row["Country"],
                state_id,
                row["State"],
                city_id,
                row["City"],
                row["Postal Code"],
                region_id,
                row["Region"],
            ),
        )

    connection.commit()
    print(f"Loaded {len(location_df)} records into Location dimension")


# Function to load Category and Product dimension tables
def load_product_dimensions(connection, df, level_mappings):
    # Extract unique categories
    category_df = df["Category"].drop_duplicates().reset_index(drop=True)

    # Insert data into Category table
    cursor = connection.cursor()
    for category in category_df:
        query = """
        INSERT INTO Category (category_name)
        VALUES (%s)
        """
        cursor.execute(query, (category,))

    connection.commit()
    print(f"Loaded {len(category_df)} records into Category dimension")

    # Get category IDs for mapping
    cursor.execute("SELECT category_id, category_name FROM Category")
    category_mapping = {row[1]: row[0] for row in cursor.fetchall()}

    # Extract unique products
    product_df = df[
        ["Product ID", "Product Name", "Category", "Sub-Category"]
    ].drop_duplicates()

    # Insert data into Product table
    for _, row in product_df.iterrows():
        category_id = category_mapping.get(row["Category"])
        sub_category_id = level_mappings["sub_category"][row["Sub-Category"]]

        query = """
        INSERT INTO Product (product_code, product_name, category_id, category_name, sub_category_id, sub_category_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            query,
            (
                row["Product ID"],
                row["Product Name"],
                category_id,
                row["Category"],
                sub_category_id,
                row["Sub-Category"],
            ),
        )

    connection.commit()
    print(f"Loaded {len(product_df)} records into Product dimension")

def load_dimension_tables(connection, df):
    # Now execute all of our dimension loading functions
    try:
        # Make sure we're connected
        if connection.is_connected():
            print("Loading dimension tables...")

            # Create level key mappings first
            level_mappings = create_level_mappings(df)

            load_calendar_dimension(connection, df)
            load_customer_dimension(connection, df)
            load_geography_dimensions(connection, df, level_mappings)
            load_product_dimensions(connection, df, level_mappings)

            print("All dimension tables loaded successfully!")
    except Error as e:
        print(f"Error: {e}")


# Function to load Item fact table
def load_item_fact_table(connection, df):
    cursor = connection.cursor()

    print("Starting ETL process for Item fact table...")

    # Step 1: Retrieve all necessary dimension keys from the database
    # Get customer mappings
    cursor.execute("SELECT customer_id, customer_code FROM Customer")
    customer_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    print(f"Loaded {len(customer_mapping)} customer mappings")

    # Get product mappings
    cursor.execute("SELECT product_id, product_code FROM Product")
    product_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    print(f"Loaded {len(product_mapping)} product mappings")

    # Get calendar mappings for order dates
    cursor.execute("SELECT calendar_id, full_date FROM Calendar")
    calendar_mapping = {
        row[1].strftime("%Y-%m-%d"): row[0] for row in cursor.fetchall()
    }
    print(f"Loaded {len(calendar_mapping)} calendar mappings")

    # Get location mappings - using postal code and city as the composite key
    cursor.execute("SELECT location_id, postal_code, city_name FROM Location")
    location_mapping = {(row[1], row[2]): row[0] for row in cursor.fetchall()}
    print(f"Loaded {len(location_mapping)} location mappings")

    # Step 2: Process each row in the dataframe
    item_count = 0
    skipped_count = 0

    for _, row in df.iterrows():
        try:
            # Format dates for lookup
            order_date = pd.to_datetime(row["Order Date"]).strftime("%Y-%m-%d")

            # Look up dimension keys
            customer_id = customer_mapping.get(row["Customer ID"])
            product_id = product_mapping.get(row["Product ID"])
            calendar_id = calendar_mapping.get(order_date)
            location_key = (str(row["Postal Code"]), row["City"])
            location_id = location_mapping.get(location_key)

            # Skip if any dimension key is missing
            if not all([customer_id, product_id, calendar_id, location_id]):
                skipped_count += 1
                if skipped_count <= 5:  # Limit the number of error messages
                    print(
                        f"Skipping record due to missing keys - Order ID: {row['Order ID']}, Product: {row['Product Name']}"
                    )
                continue

            # Calculate fact measures based on the requirements
            quantity = int(row["Quantity"])
            sales = float(row["Sales"])
            discount = float(row["Discount"])

            # Calculate lost_value (difference between full price and discounted price)
            # Full price = sales / (1 - discount)
            if discount < 1:
                full_price = sales / (1 - discount)
                lost_value = full_price - sales
            else:
                lost_value = 0  # Handle edge case of 100% discount

            profit = float(row["Profit"])

            # Insert into fact table
            query = """
            INSERT INTO Item (customer_id, location_id, calendar_id, product_id, 
                            order_code, quantity, sales, discount, lost_value, profit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(
                query,
                (
                    customer_id,
                    location_id,
                    calendar_id,
                    product_id,
                    row["Order ID"],
                    quantity,
                    sales,
                    discount,
                    lost_value,
                    profit,
                ),
            )

            item_count += 1

            # Commit in batches to improve performance
            if item_count % 500 == 0:
                connection.commit()
                print(f"Processed {item_count} items...")

        except Exception as e:
            print(f"Error processing row: {e}")
            print(f"Row data: {row['Order ID']}, {row['Product Name']}")

    # Final commit
    connection.commit()
    print(f"Loaded {item_count} records into Item fact table")
    print(f"Skipped {skipped_count} records due to missing dimension keys")

    return item_count

def load_fact_tables(connection, df):
    # Execute the loading function
    try:
        # Make sure we're connected
        if connection.is_connected():
            print("Loading Item fact table...")
            item_count = load_item_fact_table(connection, df)
            print(f"Item fact table loading complete - {item_count} records inserted")
    except Error as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    connection = connect()

    df_raw = load_data()
    
    df = preprocess_merge_duplicate_products(df_raw)
    
    load_dimension_tables(connection, df)
    load_fact_tables(connection, df)
    

    # Close the connection
    connection.close()
