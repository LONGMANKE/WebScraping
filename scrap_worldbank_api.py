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

# Create the table if it doesn't exist
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table} (
    document_id VARCHAR PRIMARY KEY,
    docty VARCHAR,
    entityid VARCHAR,
    display_title TEXT,
    pdfurl TEXT,
    listing_relative_url TEXT,
    url_friendly_title TEXT,
    new_url TEXT,
    guid TEXT,
    url TEXT,
    docdt DATE,
    other_data JSONB
);
"""
cursor.execute(create_table_query)
conn.commit()

# Base URL for the WDS API
base_url = "https://search.worldbank.org/api/v2/wds"

# Parameters for the API request
params = {
    "format": "json",
    "rows": 1000,  # Maximum number of rows per request
    "os": 0,       # Offset, starting at 0
    "fl": "id,docdt,docty,entityids,display_title,pdfurl,listing_relative_url,url_friendly_title,new_url,guid,url",  # Fields to retrieve
    "strdate": "2018-01-01",  # Start date for filtering
    "docty_exact": "Procurement Plan"  # Exact document type to filter by
}

# Total number of records to retrieve
total_records = 10055

# Function to insert data into PostgreSQL if it doesn't already exist
def insert_data(doc):
    # Check if the record already exists
    cursor.execute(f"SELECT 1 FROM {table} WHERE document_id = %s", (doc.get('id'),))
    if cursor.fetchone() is None:  # If the record doesn't exist, insert it
        cursor.execute(
            f"""
            INSERT INTO {table} (document_id, docty, entityid, display_title, pdfurl, listing_relative_url, url_friendly_title, new_url, guid, url, docdt, other_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                doc.get('id'),
                doc.get('docty'),
                doc.get('entityids', {}).get('entityid'),
                doc.get('display_title'),
                doc.get('pdfurl'),
                doc.get('listing_relative_url'),
                doc.get('url_friendly_title'),
                doc.get('new_url'),
                doc.get('guid'),
                doc.get('url'),
                doc.get('docdt'),
                json.dumps(doc)  # Insert the full document as JSON
            )
        )
        conn.commit()
        print(f"Inserted document ID {doc.get('id')}")
    else:
        print(f"Document ID {doc.get('id')} already exists, skipping insert.")

# Loop through all pages to retrieve the data
while params["os"] < total_records:
    # Make the API request
    response = requests.get(base_url, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        documents = data.get('documents', {})
        
        # Remove facets key if present
        if 'facets' in documents:
            documents.pop('facets')
        
        # Process and insert each document into the database
        for doc_id, doc_info in documents.items():
            insert_data(doc_info)
        
        # Print progress
        print(f"Retrieved and inserted records {params['os']} to {params['os'] + params['rows']} of {total_records}.")
        
        # Update the offset for the next request
        params["os"] += params["rows"]
    else:
        print(f"Request failed with status code {response.status_code}")
        break

# Close the database connection
cursor.close()
conn.close()

print(f"All documents have been retrieved and inserted into the '{table}' table.")
