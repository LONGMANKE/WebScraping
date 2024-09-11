import psycopg2
import pandas as pd

# Database connection parameters
db_name = 'scraper'
db_user = 'postgres'
db_password = '12345'
db_host = 'localhost'
db_port = '5432'

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cursor = conn.cursor()

# SQL query to select data from world_bank1 and world_bank (world_bank2)
query = """
SELECT 
    wb.id AS world_bank_id,
    wb.docdt AS document_date,
    wb.docty AS document_type,
    wb.display_title AS display_title,
    wb.pdfurl AS pdf_url,
    wb.listing_relative_url AS listing_url,
    wb.code AS project_id,
    wbn.id AS notice_id,
    wbn.notice_type,
    wbn.noticedate AS notice_date,
    wbn.notice_lang_name,
    wbn.notice_status,
    wbn.submission_deadline_date,
    wbn.submission_deadline_time,
    wbn.project_ctry_name,
    wbn.project_name,
    wbn.bid_reference_no,
    wbn.bid_description,
    wbn.procurement_group,
    wbn.procurement_method_code,
    wbn.procurement_method_name,
    wbn.contact_address,
    wbn.contact_ctry_name,
    wbn.contact_email,
    wbn.contact_name,
    wbn.contact_organization,
    wbn.contact_phone_no,
    wbn.submission_date
FROM world_bank2 wb
JOIN world_bank wbn
ON wb.code = wbn.project_id;
"""

try:
    # Execute the SQL query
    cursor.execute(query)

    # Fetch all rows from the executed query
    rows = cursor.fetchall()

    # Get column names for the DataFrame
    column_names = [desc[0] for desc in cursor.description]

    # Convert the result into a pandas DataFrame for better display and manipulation
    df = pd.DataFrame(rows, columns=column_names)

    # Display the DataFrame
    print(df)

except Exception as e:
    print(f"Error executing the query: {e}")
finally:
    # Close the database connection
    cursor.close()
    conn.close()
