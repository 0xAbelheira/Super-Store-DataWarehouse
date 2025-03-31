import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("excel_export.log"), logging.StreamHandler()],
)
logger = logging.getLogger("excel_export")

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
db_config = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


def get_tables(connection):
    """Get list of all tables in the database"""
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    return tables


def get_table_data(connection, table_name):
    """Get all data from a specified table"""
    query = f"SELECT * FROM {table_name}"
    try:
        df = pd.read_sql(query, connection)
        logger.info(f"Retrieved {len(df)} rows from {table_name}")
        return df
    except Error as e:
        logger.error(f"Error reading data from {table_name}: {e}")
        return pd.DataFrame()  # Return empty dataframe on error


def main():
    try:
        # Connect to the MySQL database
        logger.info("Connecting to database...")
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            logger.info(f"Connected to MySQL database: {db_config['database']}")

            # Get all tables
            tables = get_tables(connection)
            logger.info(f"Found {len(tables)} tables in database")

            # Create Excel writer object with XlsxWriter engine
            output_file = f"{db_config['database']}_export.xlsx"
            logger.info(f"Creating Excel file: {output_file}")

            # Use ExcelWriter with xlsxwriter engine for better formatting options
            with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                # Create separate worksheets for dimension and fact tables for better organization
                for table_name in tables:
                    # Get data from table
                    df = get_table_data(connection, table_name)

                    if not df.empty:
                        # Write dataframe to Excel sheet
                        df.to_excel(writer, sheet_name=table_name, index=False)

                        # Get the xlsxwriter workbook and worksheet objects
                        workbook = writer.book
                        worksheet = writer.sheets[table_name]

                        # Add formatting
                        header_format = workbook.add_format(
                            {
                                "bold": True,
                                "text_wrap": True,
                                "valign": "top",
                                "fg_color": "#D7E4BC",
                                "border": 1,
                            }
                        )

                        # Write the column headers with the defined format
                        for col_num, value in enumerate(df.columns.values):
                            worksheet.write(0, col_num, value, header_format)

                        # Set column widths
                        for i, col in enumerate(df.columns):
                            # Set column width based on max length of data in column
                            max_len = (
                                max(df[col].astype(str).map(len).max(), len(col)) + 2
                            )
                            worksheet.set_column(
                                i, i, min(max_len, 30)
                            )  # Cap width at 30

            logger.info(f"Excel file created successfully: {output_file}")

    except Error as e:
        logger.error(f"Database connection error: {e}")

    finally:
        if "connection" in locals() and connection.is_connected():
            connection.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()
