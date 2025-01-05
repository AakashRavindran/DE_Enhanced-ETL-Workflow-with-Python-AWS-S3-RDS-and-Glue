import boto3
import pandas as pd
import pymysql
import os

# Configuration for AWS S3 and MySQL
S3_BUCKET_NAME = "***BUCKET***"
S3_FILE_KEY = "***BUCKET***/file.csv"
RDS_HOST = "***RDS***"
RDS_PORT = 3306  # Default MySQL port
RDS_DATABASE = "database"
RDS_USER = "admin"
RDS_PASSWORD = "***password***"
TABLE_NAME = "mysqltable***"

# Initialize S3 client
s3_client = boto3.client('s3')


# Function to download file from S3
def download_csv_from_s3(bucket_name, file_key, download_path):
    try:
        s3_client.download_file(bucket_name, file_key, download_path)
        print(f"File {file_key} downloaded from S3 to {download_path}")
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        raise


# Function to upload data from DataFrame to MySQL
def upload_csv_to_mysql(df, table_name):
    try:
        # Connect to MySQL
        connection = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASSWORD,
            database=RDS_DATABASE,
            port=RDS_PORT
        )

        cursor = connection.cursor()

        # Create table if it does not exist
        # Build the CREATE TABLE statement dynamically from DataFrame columns
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for column in df.columns:
            create_table_query += f"`{column}` VARCHAR(255),"
        create_table_query = create_table_query.rstrip(',') + ");"
        cursor.execute(create_table_query)
        print(f"Table {table_name} created or already exists in the database.")

        # Insert data into the table
        for i, row in df.iterrows():
            # Create an insert query dynamically for each row
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(row))})"
            cursor.execute(insert_query, tuple(row))

        # Commit the changes to the database
        connection.commit()
        print(f"Data inserted into {table_name}.")

    except Exception as e:
        print(f"Error uploading data to MySQL: {e}")
        raise
    finally:
        cursor.close()
        connection.close()


# Main process
def main():
    # Step 1: Download the CSV from S3
    download_path = '/home/ubuntu/s3Download/temp.csv'  # Temporary file location on local machine
    download_csv_from_s3(S3_BUCKET_NAME, S3_FILE_KEY, download_path)

    # Step 2: Read the CSV into a Pandas DataFrame
    df = pd.read_csv(download_path)
    print(f"CSV file {S3_FILE_KEY} read into DataFrame.")

    # Step 3: Upload the data from the DataFrame to MySQL
    upload_csv_to_mysql(df, TABLE_NAME)

    # Clean up the downloaded file (optional)
    os.remove(download_path)
    print(f"Temporary file {download_path} removed.")


if __name__ == "__main__":
    main()
