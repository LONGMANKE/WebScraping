import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# Step 1: Load Data from All Excel Files in Directory

# Specify the directory containing the downloaded Excel files
download_dir = r"C:\worldbank"

# Step 2: Connect to PostgreSQL Database

# Update these details with your PostgreSQL database credentials
db_name = 'scraper'
db_user = 'postgres'
db_password = '12345'
db_host = 'localhost'   
db_port = '5432'        

try:
    # Create an SQLAlchemy engine to connect to PostgreSQL
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    connection = engine.connect()
    print("Connected to the PostgreSQL database successfully.")
except Exception as e:
    print(f"Failed to connect to the PostgreSQL database: {e}")
    exit()

# Step 3: Loop Through Each Excel File and Push Data to PostgreSQL

# Update this with your target table name
table_name = 'world_bank'

try:
    # List all Excel files in the download directory
    excel_files = [file for file in os.listdir(download_dir) if file.endswith(('.xls', '.xlsx'))]

    for file in excel_files:
        file_path = os.path.join(download_dir, file)
        try:
            # Read each Excel file
            df_excel = pd.read_excel(file_path)
            print(f"Excel file '{file}' loaded successfully:")
            print(df_excel.head())

            # Write data to PostgreSQL table
            # Use if_exists='append' to add data to the table without overwriting it
            df_excel.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Data from '{file}' pushed to the PostgreSQL table '{table_name}' successfully.")
        except Exception as e:
            print(f"An error occurred while processing file '{file}': {e}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the database connection
    connection.close()
    print("Database connection closed.") 
