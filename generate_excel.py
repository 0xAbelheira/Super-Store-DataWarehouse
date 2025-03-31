#!/usr/bin/env python3
# filepath: /Users/brunofernandes/Desktop/FEUP/MECD/ucs/2S/AD/project/Super-Store-DataWarehouse/generate_excel.py

import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine

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
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


def get_tables(engine):
    """Get list of all tables in the database"""
    query = "SHOW TABLES"
    tables_df = pd.read_sql(query, engine)
    tables = tables_df.iloc[:, 0].tolist()
    return tables


def get_table_data(engine, table_name):
    """Get all data from a specified table"""
    query = f"SELECT * FROM {table_name}"
    try:
        df = pd.read_sql(query, engine)
        logger.info(f"Retrieved {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error reading data from {table_name}: {e}")
        return pd.DataFrame()  # Return empty dataframe on error


def main():
    try:
        # Create SQLAlchemy engine
        logger.info("Creating database connection...")
        connection_string = (
            f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
        )
        engine = create_engine(connection_string)

        # Test connection
        with engine.connect() as connection:
            logger.info(f"Connected to MySQL database: {db_name}")

            # Get all tables
            tables = get_tables(engine)
            logger.info(f"Found {len(tables)} tables in database")

            # Create Excel writer object with XlsxWriter engine
            output_file = f"{db_name}_export.xlsx"
            logger.info(f"Creating Excel file: {output_file}")

            # Use ExcelWriter with xlsxwriter engine for better formatting options
            with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                # Create separate worksheets for dimension and fact tables for better organization
                for table_name in tables:
                    # Get data from table
                    df = get_table_data(engine, table_name)

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

    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()
