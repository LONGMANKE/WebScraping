import os
import pandas as pd
from sqlalchemy import create_engine, text

# Step 1: Specify the directory containing the downloaded Excel files
download_dir = r"C:\worldbank"

# Step 2: Connect to PostgreSQL Database

# Update these details with your PostgreSQL database credentials
db_name = 'scraper'
db_user = 'postgres'
db_password = '12345'
db_host = 'localhost'  # Or your host
db_port = '5432'       # Default PostgreSQL port

# Create an SQLAlchemy engine to connect to PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

def create_table_if_not_exists(engine, table_name):
    """
    Create the table with an auto-incrementing 'id' column if it doesn't already exist.
    """
    with engine.connect() as connection:
        # Use sqlalchemy.text to handle raw SQL
        result = connection.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        """))
        table_exists = result.scalar()
        
        if not table_exists:
            connection.execute(text(f"""
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    "Description" TEXT,
                    "Country" TEXT,
                    "Project Title" TEXT,
                    "Notice Type" TEXT,
                    "Language" TEXT,
                    "notice_lang_name" TEXT,
                    "submission_date" DATE
                );
            """))
            # Commit the transaction to ensure table creation is recognized
            connection.commit()
            print(f"Table '{table_name}' created successfully.")

def load_and_push_data_to_db(engine, df, table_name):
    """
    Push data to PostgreSQL table.
    """
    try:
        # Fetch existing descriptions from the database to avoid duplicates
        existing_descriptions_query = text(f"SELECT \"Description\" FROM {table_name};")
        existing_descriptions_df = pd.read_sql(existing_descriptions_query, engine)
        existing_descriptions_set = set(existing_descriptions_df["Description"].unique())

        # Filter out rows with existing descriptions
        new_data = df[~df["Description"].isin(existing_descriptions_set)]

        if not new_data.empty:
            # Write new data to PostgreSQL table, skipping the `id` column
            new_data.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Data pushed to the PostgreSQL table '{table_name}' successfully.")
        else:
            print(f"No new data to add to '{table_name}'.")

    except Exception as e:
        print(f"An error occurred while pushing data to '{table_name}': {e}")
    finally:
        print("Data processing completed.")

# Define table names
world_bank_table = 'world_bank'
new_table = 'new_worldbank_rfps'

# Create tables if they do not exist
create_table_if_not_exists(engine, world_bank_table)
create_table_if_not_exists(engine, new_table)

# Step 3: Loop through all Excel files in the specified directory

excel_files = [file for file in os.listdir(download_dir) if file.endswith(('.xls', '.xlsx'))]

for excel_file in excel_files:
    file_path = os.path.join(download_dir, excel_file)
    
    try:
        # Load the Excel file
        df_excel = pd.read_excel(file_path)
        print(f"Excel file '{excel_file}' loaded successfully:")
        print(df_excel.head())
        
        # Load data to both tables
        load_and_push_data_to_db(engine, df_excel, world_bank_table)
        load_and_push_data_to_db(engine, df_excel, new_table)
    
    except Exception as e:
        print(f"An error occurred while processing the file '{excel_file}': {e}")
