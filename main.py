import requests
import psycopg2
import psycopg2.extras

# Function to fetch data from the API
def fetch_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data from API. Status code: {response.status_code}")
        return []

# Function to create a table with dynamic columns
def create_table(conn, table_name, columns):
    cursor = conn.cursor()
    
    # Construct the CREATE TABLE query dynamically
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    create_table_query += ", ".join([f"{column} TEXT" for column in columns])
    create_table_query += ");"
    
    try:
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully or already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()

# Function to save data to PostgreSQL dynamically
def save_to_db(data, conn, table_name):
    if not data:
        print("No data to save.")
        return
    
    # Use the first record to determine the columns
    columns = data[0].keys()
    
    # Create the table dynamically
    create_table(conn, table_name, columns)
    
    # Construct the INSERT query dynamically
    cursor = conn.cursor()
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    
    # Convert records to tuples
    values = [[record.get(col, None) for col in columns] for record in data]
    
    try:
        psycopg2.extras.execute_values(cursor, insert_query, values)
        conn.commit()
        print("Data inserted successfully!")
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()

# Main function to process the API endpoint
def process_api_endpoint(api_url, db_credentials, table_name="procurement_opportunity"):
    # Fetch data from the API
    api_data = fetch_data(api_url)
    if not api_data:
        print("No data fetched from API.")
        return
    
    # Connect to PostgreSQL database
    try:
        conn = psycopg2.connect(**db_credentials)
        print("Connected to the database successfully!")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return
    
    # Save the data to the database dynamically
    save_to_db(api_data, conn, table_name)
    
    # Close the connection
    conn.close()

# Replace with your actual database credentials
db_credentials = {
    "dbname": "scraper",
    "user": "postgres",
    "password": "12345",
    "host": "localhost",
    "port": "5432"
}

# Example usage
api_endpoint = "https://finances.worldbank.org/resource/8wu6-c7c9.json"
process_api_endpoint(api_endpoint, db_credentials)