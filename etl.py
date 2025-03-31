# Connect to the MySQL database using .env file
import os
import logging
from datetime import datetime

import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from mysql.connector import Error

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_process.log"), logging.StreamHandler()],
)

logger = logging.getLogger("superstore_etl")

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
            logger.info(f"Connected to MySQL Server version {db_info}")
            cursor = connection_db.cursor()
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            logger.info(f"Connected to database: {record[0]}")

    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")

    return connection_db


def load_data():
    # Read the CSV file
    df = pd.read_csv("Sample - Superstore.csv", encoding="windows-1252")

    # Log column names
    logger.info(f"Column names: {df.columns.tolist()}")

    return df


def preprocess_merge_duplicate_products(df):
    """
    Preprocess the dataset to merge rows that have the same product in the same order.

    Args:
        df: The original dataframe containing Superstore data

    Returns:
        Preprocessed dataframe with merged duplicate product entries
    """
    logger.info(f"Original dataset shape: {df.shape}")

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

    logger.info(
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

        logger.info(f"After merging duplicates, dataset shape: {processed_df.shape}")

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
    logger.info(f"Loaded {len(month_data)} records into CalendarMonth dimension")

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
    logger.info(f"Loaded {len(calendar_df)} records into Calendar dimension")

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
    logger.info(f"Loaded {len(customer_df)} records into Customer dimension")


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
    logger.info(f"Loaded {len(region_df)} records into Region dimension")

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
    logger.info(f"Loaded {len(state_df)} records into State dimension")

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
    logger.info(f"Loaded {len(location_df)} records into Location dimension")


# Function to load Shipping dimension table
def load_shipping_dimension(connection, df):
    # Extract unique shipping modes
    shipping_df = df["Ship Mode"].drop_duplicates().reset_index(drop=True)

    cursor = connection.cursor()
    for shipping_mode in shipping_df:
        query = """
        INSERT INTO Shipping (ship_mode) 
        VALUES (%s)
        """
        cursor.execute(query, (shipping_mode,))

    connection.commit()
    logger.info(f"Loaded {len(shipping_df)} records into Shipping dimension")


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
    logger.info(f"Loaded {len(category_df)} records into Category dimension")

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
    logger.info(f"Loaded {len(product_df)} records into Product dimension")


def load_dimension_tables(connection, df):
    # Now execute all of our dimension loading functions
    try:
        # Make sure we're connected
        if connection.is_connected():
            logger.info("Loading dimension tables...")

            # Create level key mappings first
            level_mappings = create_level_mappings(df)

            load_calendar_dimension(connection, df)
            load_customer_dimension(connection, df)
            load_geography_dimensions(connection, df, level_mappings)
            load_shipping_dimension(connection, df)
            load_product_dimensions(connection, df, level_mappings)

            logger.info("All dimension tables loaded successfully!")
    except Error as e:
        logger.error(f"Error loading dimension tables: {e}")


# Function to load Item fact table
def load_item_fact_table(connection, df):
    cursor = connection.cursor()

    logger.info("Starting ETL process for Item fact table...")

    # Step 1: Retrieve all necessary dimension keys from the database
    # Get customer mappings
    cursor.execute("SELECT customer_id, customer_code FROM Customer")
    customer_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    logger.info(f"Loaded {len(customer_mapping)} customer mappings")

    # Get product mappings
    cursor.execute("SELECT product_id, product_code FROM Product")
    product_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    logger.info(f"Loaded {len(product_mapping)} product mappings")

    # Get calendar mappings for order dates
    cursor.execute("SELECT calendar_id, full_date FROM Calendar")
    calendar_mapping = {
        row[1].strftime("%Y-%m-%d"): row[0] for row in cursor.fetchall()
    }
    logger.info(f"Loaded {len(calendar_mapping)} calendar mappings")

    # Get location mappings - using postal code and city as the composite key
    cursor.execute("SELECT location_id, postal_code, city_name FROM Location")
    location_mapping = {(row[1], row[2]): row[0] for row in cursor.fetchall()}
    logger.info(f"Loaded {len(location_mapping)} location mappings")

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
                    logger.warning(
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
                logger.info(f"Processed {item_count} items...")

        except Exception as e:
            logger.error(f"Error processing row: {e}")
            logger.error(f"Row data: {row['Order ID']}, {row['Product Name']}")

    # Final commit
    connection.commit()
    logger.info(f"Loaded {item_count} records into Item fact table")
    logger.info(f"Skipped {skipped_count} records due to missing dimension keys")

    return item_count


# Function to load Orders fact table
def load_orders_fact_table(connection, df):
    cursor = connection.cursor()

    logger.info("Starting ETL process for Orders fact table...")

    # Step 1: Retrieve all necessary dimension keys from the database
    # Get customer mappings
    cursor.execute("SELECT customer_id, customer_code FROM Customer")
    customer_mapping = {row[1]: row[0] for row in cursor.fetchall()}

    # Get calendar mappings for order and shipping dates
    cursor.execute("SELECT calendar_id, full_date FROM Calendar")
    calendar_mapping = {
        row[1].strftime("%Y-%m-%d"): row[0] for row in cursor.fetchall()
    }

    # Get location mappings
    cursor.execute("SELECT location_id, postal_code, city_name FROM Location")
    location_mapping = {(row[1], row[2]): row[0] for row in cursor.fetchall()}

    # Get shipping mappings
    cursor.execute("SELECT shipping_id, ship_mode FROM Shipping")
    shipping_mapping = {row[1]: row[0] for row in cursor.fetchall()}

    # Step 2: Group data by order to calculate order-level measures
    # We need to aggregate by order ID
    order_groups = df.groupby("Order ID")

    order_count = 0
    skipped_count = 0

    for order_id, order_data in order_groups:
        try:
            # Take the first row for order-level data (dates, customer, location, shipping)
            first_row = order_data.iloc[0]

            # Format dates for lookup
            order_date = pd.to_datetime(first_row["Order Date"]).strftime("%Y-%m-%d")
            ship_date = pd.to_datetime(first_row["Ship Date"]).strftime("%Y-%m-%d")

            # Look up dimension keys
            customer_id = customer_mapping.get(first_row["Customer ID"])
            order_calendar_id = calendar_mapping.get(order_date)
            shipping_calendar_id = calendar_mapping.get(ship_date)
            location_key = (str(first_row["Postal Code"]), first_row["City"])
            location_id = location_mapping.get(location_key)
            shipping_id = shipping_mapping.get(first_row["Ship Mode"])

            # Skip if any dimension key is missing
            if not all(
                [
                    customer_id,
                    order_calendar_id,
                    shipping_calendar_id,
                    location_id,
                    shipping_id,
                ]
            ):
                skipped_count += 1
                if skipped_count <= 5:  # Limit the number of error messages
                    logger.warning(
                        f"Skipping order due to missing keys - Order ID: {order_id}"
                    )
                continue

            # Calculate fact measures based on the requirements
            # Aggregate values across all items in the order
            quantity_order = int(order_data["Quantity"].sum())  # Convert to Python int
            sales_order = float(order_data["Sales"].sum())      # Convert to Python float
            profit_order = float(order_data["Profit"].sum())    # Convert to Python float

            # Calculate lost_value_order (full price - discounted price for the entire order)
            # We need to calculate this for each item and then sum
            lost_value_order = 0.0  # Use native Python float
            for _, item in order_data.iterrows():
                discount = float(item["Discount"])
                item_sales = float(item["Sales"])

                if discount < 1:
                    full_price = item_sales / (1 - discount)
                    lost_value_order += (full_price - item_sales)

            # Convert the final lost_value to ensure it's a native Python float
            lost_value_order = float(lost_value_order)

            # Insert into Orders fact table
            query = """
            INSERT INTO Orders (order_calendar_id, shipping_calendar_id, customer_id, location_id, 
                            shipping_id, order_code, sales_order, quantity_order, 
                            lost_value_order, profit_order)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(
                query,
                (
                    order_calendar_id,
                    shipping_calendar_id,
                    customer_id,
                    location_id,
                    shipping_id,
                    order_id,
                    sales_order,
                    quantity_order,
                    lost_value_order,
                    profit_order,
                ),
            )

            order_count += 1

            # Commit in batches to improve performance
            if order_count % 200 == 0:
                connection.commit()
                logger.info(f"Processed {order_count} orders...")

        except Exception as e:
            logger.error(f"Error processing order: {e}")
            logger.error(f"Order data: {order_id}")

    # Final commit
    connection.commit()
    logger.info(f"Loaded {order_count} records into Orders fact table")
    logger.info(f"Skipped {skipped_count} orders due to missing dimension keys")

    return order_count


# Function to load OrderM fact table
def load_order_m_fact_table(connection, df):
    cursor = connection.cursor()

    logger.info("Starting ETL process for OrderM fact table...")

    # Step 1: Retrieve dimension keys from the database
    # Get CalendarMonth mappings
    cursor.execute(
        """
        SELECT cm.calendar_month_id, cm.year_number, cm.calendar_month_number 
        FROM CalendarMonth cm
    """
    )
    calendar_month_mapping = {(row[1], row[2]): row[0] for row in cursor.fetchall()}
    logger.info(f"Loaded {len(calendar_month_mapping)} calendar month mappings")

    # Get State mappings
    cursor.execute("SELECT state_id, state_name FROM State")
    state_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    logger.info(f"Loaded {len(state_mapping)} state mappings")

    # Step 2: Create month and year columns for grouping
    df["year"] = pd.to_datetime(df["Order Date"]).dt.year
    df["month"] = pd.to_datetime(df["Order Date"]).dt.month

    # Step 3: Group data by year, month, and state to calculate aggregates
    grouped_data = (
        df.groupby(["year", "month", "State"])
        .agg({"Sales": "sum", "Quantity": "sum", "Profit": "sum"})
        .reset_index()
    )

    # Step 4: Calculate lost_value_month from original data
    monthly_lost_value = {}

    for _, row in df.iterrows():
        # Extract date components
        date = pd.to_datetime(row["Order Date"])
        year = date.year
        month = date.month
        state = row["State"]

        # Calculate lost value for this item
        discount = float(row["Discount"])
        sales = float(row["Sales"])

        if discount < 1:
            full_price = sales / (1 - discount)
            lost_value = full_price - sales
        else:
            lost_value = 0

        # Aggregate by (year, month, state)
        key = (year, month, state)
        if key not in monthly_lost_value:
            monthly_lost_value[key] = 0
        monthly_lost_value[key] += lost_value

    # Step 5: Insert data into OrderM table
    inserted_count = 0
    skipped_count = 0

    for _, row in grouped_data.iterrows():
        try:
            year = row["year"]
            month = row["month"]
            state_name = row["State"]

            # Get dimension keys
            calendar_month_id = calendar_month_mapping.get((year, month))
            state_id = state_mapping.get(state_name)

            if not all([calendar_month_id, state_id]):
                skipped_count += 1
                if skipped_count <= 5:
                    logger.warning(
                        f"Skipping OrderM record due to missing keys - Year: {year}, Month: {month}, State: {state_name}"
                    )
                continue

            # Get the measures
            sales_month = float(row["Sales"])
            quantity_month = float(row["Quantity"])  # Using float as per table schema
            profit_month = float(row["Profit"])

            # Get lost value from the dictionary we calculated earlier
            key = (year, month, state_name)
            lost_value_month = float(monthly_lost_value.get(key, 0))

            # Insert into OrderM table
            query = """
            INSERT INTO OrderM (calendar_month_id, state_id, sales_month, quantity_month, 
                              lost_value_month, profit_month)
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            cursor.execute(
                query,
                (
                    calendar_month_id,
                    state_id,
                    sales_month,
                    quantity_month,
                    lost_value_month,
                    profit_month,
                ),
            )

            inserted_count += 1

            # Commit in batches
            if inserted_count % 50 == 0:
                connection.commit()
                logger.info(f"Processed {inserted_count} monthly aggregations...")

        except Exception as e:
            logger.error(f"Error processing OrderM record: {e}")
            logger.error(
                f"Record data: Year: {year}, Month: {month}, State: {state_name}"
            )

    # Final commit
    connection.commit()
    logger.info(f"Loaded {inserted_count} records into OrderM fact table")
    logger.info(f"Skipped {skipped_count} records due to missing dimension keys")

    return inserted_count


# Function to load ProductPerformance fact table
def load_product_performance_fact_table(connection, df):
    cursor = connection.cursor()

    logger.info("Starting ETL process for ProductPerformance fact table...")

    # Step 1: Retrieve dimension keys from the database
    # Get Category mappings
    cursor.execute("SELECT category_id, category_name FROM Category")
    category_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    logger.info(f"Loaded {len(category_mapping)} category mappings")

    # Get State mappings
    cursor.execute("SELECT state_id, state_name FROM State")
    state_mapping = {row[1]: row[0] for row in cursor.fetchall()}
    logger.info(f"Loaded {len(state_mapping)} state mappings")

    # Get CalendarMonth mappings
    cursor.execute(
        """
        SELECT calendar_month_id, year_number, calendar_month_number 
        FROM CalendarMonth
    """
    )
    calendar_month_mapping = {(row[1], row[2]): row[0] for row in cursor.fetchall()}
    logger.info(f"Loaded {len(calendar_month_mapping)} calendar month mappings")

    # Step 2: Create month and year columns for grouping
    df["year"] = pd.to_datetime(df["Order Date"]).dt.year
    df["month"] = pd.to_datetime(df["Order Date"]).dt.month

    # Step 3: Group data by category, state, year, month to calculate aggregates
    grouped_data = (
        df.groupby(["Category", "State", "year", "month"])
        .agg({"Sales": "sum", "Profit": "sum", "Quantity": "sum"})
        .reset_index()
    )

    # Step 4: Calculate cumulative profit per category and state over time
    # Initialize a dictionary to store cumulative profit
    cumulative_profits = {}

    # Sort the data by year, month to ensure correct cumulative calculation
    grouped_data = grouped_data.sort_values(["Category", "State", "year", "month"])

    # Calculate cumulative profit for each category and state
    for _, row in grouped_data.iterrows():
        category = row["Category"]
        state = row["State"]
        profit = float(row["Profit"])

        # Create a key for this category-state combination
        key = (category, state)

        # Initialize if not exists
        if key not in cumulative_profits:
            cumulative_profits[key] = 0

        # Add current profit to cumulative
        cumulative_profits[key] += profit

        # Store the cumulative value back in the row
        row["cumulative_profit"] = cumulative_profits[key]

    # Step 5: Insert data into ProductPerformance table
    inserted_count = 0
    skipped_count = 0
    
    # log the cumulative profits for debugging
    logger.info(f"Cumulative profits: {grouped_data[:,['cumulative_profit']]}")

    for _, row in grouped_data.iterrows():
        try:
            category_name = row["Category"]
            state_name = row["State"]
            year = row["year"]
            month = row["month"]

            # Get dimension keys
            category_id = category_mapping.get(category_name)
            state_id = state_mapping.get(state_name)
            calendar_month_id = calendar_month_mapping.get((year, month))

            if not all([category_id, state_id, calendar_month_id]):
                skipped_count += 1
                if skipped_count <= 5:
                    logger.warning(
                        f"Skipping ProductPerformance record due to missing keys - Category: {category_name}, State: {state_name}, Year: {year}, Month: {month}"
                    )
                continue

            # Get the measures
            total_sales = float(row["Sales"])
            total_profit = float(row["Profit"])
            cumulative_profit = float(row["cumulative_profit"])
            total_quantity = int(row["Quantity"])

            # Insert into ProductPerformance table
            query = """
            INSERT INTO ProductPerformance (category_id, state_id, calendar_month_id, 
                                          total_sales, total_profit, cumulative_profit, total_quantity)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(
                query,
                (
                    category_id,
                    state_id,
                    calendar_month_id,
                    total_sales,
                    total_profit,
                    cumulative_profit,
                    total_quantity,
                ),
            )

            inserted_count += 1

            # Commit in batches
            if inserted_count % 50 == 0:
                connection.commit()
                logger.info(
                    f"Processed {inserted_count} product performance records..."
                )

        except Exception as e:
            logger.error(f"Error processing ProductPerformance record: {e}")
            logger.error(
                f"Record data: Category: {category_name}, State: {state_name}, Year: {year}, Month: {month}"
            )

    # Final commit
    connection.commit()
    logger.info(f"Loaded {inserted_count} records into ProductPerformance fact table")
    logger.info(f"Skipped {skipped_count} records due to missing dimension keys")

    return inserted_count


def load_fact_tables(connection, df):
    # Execute the loading functions
    try:
        # Make sure we're connected
        if connection.is_connected():
            # Load Item fact table
            logger.info("Loading Item fact table...")
            item_count = load_item_fact_table(connection, df)
            logger.info(
                f"Item fact table loading complete - {item_count} records inserted"
            )

            # Load Orders fact table
            logger.info("Loading Orders fact table...")
            order_count = load_orders_fact_table(connection, df)
            logger.info(
                f"Orders fact table loading complete - {order_count} records inserted"
            )

            # Load OrderM fact table
            logger.info("Loading OrderM fact table...")
            order_m_count = load_order_m_fact_table(connection, df)
            logger.info(
                f"OrderM fact table loading complete - {order_m_count} records inserted"
            )

            # Load ProductPerformance fact table
            logger.info("Loading ProductPerformance fact table...")
            product_perf_count = load_product_performance_fact_table(connection, df)
            logger.info(
                f"ProductPerformance fact table loading complete - {product_perf_count} records inserted"
            )

    except Error as e:
        logger.error(f"Error loading fact tables: {e}")


if __name__ == "__main__":
    logger.info("Starting ETL process for Superstore data warehouse")

    connection = connect()

    df_raw = load_data()

    df = preprocess_merge_duplicate_products(df_raw)

    load_dimension_tables(connection, df)
    load_fact_tables(connection, df)

    # Close the connection
    connection.close()
    logger.info("ETL process completed successfully")
