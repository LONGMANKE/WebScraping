import requests
import psycopg2
import json

# Database connection parameters
db_name = 'scraper'
db_user = 'postgres'
db_password = '12345'
db_host = 'localhost'
db_port = '5432'
table = 'world_bank'

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cursor = conn.cursor()

# Base URL for the World Bank API
base_url = "https://search.worldbank.org/api/v2/procnotices"

# Parameters for the API request
params = {
    "format": "json",
    "rows": 1000,  # Fetching up to 100 records per request
    "os": 0,    # Starting offset
    "strdate": "2024-01-01",  # Start date filter
}

# Function to validate and sanitize the data before inserting into PostgreSQL
def sanitize_data(value):
    # If the value is a dictionary or complex data type, return None
    if isinstance(value, (dict, list)):
        return None
    # If value is None, return it directly
    elif value is None:
        return None
    # If value is a string, remove unwanted characters or whitespaces
    elif isinstance(value, str):
        return value.strip()
    # Return the value as is for other types (int, float, etc.)
    else:
        return value

# Function to insert data into PostgreSQL
def insert_data(doc):
    try:
        # Sanitize each field before inserting
        sanitized_data = (
            sanitize_data(doc.get('id')),
            sanitize_data(doc.get('notice_type')),
            sanitize_data(doc.get('noticedate')),
            sanitize_data(doc.get('notice_lang_name')),
            sanitize_data(doc.get('notice_status')),
            sanitize_data(doc.get('submission_deadline_date')),
            sanitize_data(doc.get('submission_deadline_time')),
            sanitize_data(doc.get('project_ctry_name')),
            sanitize_data(doc.get('project_id')),
            sanitize_data(doc.get('project_name')),
            sanitize_data(doc.get('bid_reference_no')),
            sanitize_data(doc.get('bid_description')),
            sanitize_data(doc.get('procurement_group')),
            sanitize_data(doc.get('procurement_method_code')),
            sanitize_data(doc.get('procurement_method_name')),
            sanitize_data(doc.get('contact_address')),
            sanitize_data(doc.get('contact_ctry_name')),
            sanitize_data(doc.get('contact_email')),
            sanitize_data(doc.get('contact_name')),
            sanitize_data(doc.get('contact_organization')),
            sanitize_data(doc.get('contact_phone_no')),
            sanitize_data(doc.get('submission_date'))
        )

        # Log the sanitized data for debugging
        print("Inserting sanitized data:", sanitized_data)
        
        # Insert sanitized data into the database
        cursor.execute(
            f"""
            INSERT INTO {table} 
            (id, notice_type, noticedate, notice_lang_name, notice_status, 
             submission_deadline_date, submission_deadline_time, project_ctry_name, 
             project_id, project_name, bid_reference_no, bid_description, 
             procurement_group, procurement_method_code, procurement_method_name, 
             contact_address, contact_ctry_name, contact_email, contact_name, 
             contact_organization, contact_phone_no, submission_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;  -- Avoid duplicates
            """,
            sanitized_data
        )
        conn.commit()  # Commit after each successful insert
        print(f"Inserted notice with ID: {doc.get('id')}")
    except Exception as e:
        print(f"Error inserting data for ID {doc.get('id')}: {e}")

# Loop to handle pagination and fetch all data
while True:
    # Make the API request
    response = requests.get(base_url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()

        # Print out the raw response for debugging
        print(f"Fetching batch starting at offset: {params['os']}")

        procnotices = data.get('procnotices', [])

        # If there are no more notices, break the loop
        if not procnotices:
            print("No more procurement notices found. Ending the loop.")
            break

        # Insert each document into the database, excluding 'notice_text'
        for doc in procnotices:
            # Skip over the notice_text field and insert other data
            doc.pop('notice_text', None)  # Ensure 'notice_text' is removed
            insert_data(doc)

        # Update the offset for the next request
        params["os"] += params["rows"]
    else:
        print(f"Request failed with status code {response.status_code}")
        break

# Close the database connection
cursor.close()
conn.close()

print("All procurement notices have been retrieved and inserted.")
