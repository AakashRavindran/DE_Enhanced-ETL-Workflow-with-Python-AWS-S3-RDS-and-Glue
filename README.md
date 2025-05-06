ETL Pipeline Project
This project provides an ETL (Extract, Transform, Load) pipeline that integrates with AWS services, specifically S3, and a MySQL database. It consists of two primary components:

File Transformation and Upload to S3 (main.py)
Download from S3 and Upload to MySQL (ETL.py)
The primary goal of the project is to automate the process of transforming files (CSV, JSON, XML) stored in AWS S3, applying unit conversions 
(height from inches to meters, weight from pounds to kilograms), and then uploading the transformed files back to S3. Additionally, the project supports downloading CSV data from S3 and inserting it into a MySQL database.


Table of Contents
Project Overview
Setup
Usage
Files
License
Project Overview
The project contains two scripts:

1. main.py
Objective: This script reads and transforms data files (CSV, JSON, XML) from a local folder, converts specific columns (height and weight), and then uploads the transformed data to an S3 bucket.
Key Features:
Conversion of height from inches to meters and weight from pounds to kilograms.
Supports CSV, JSON, and XML formats.
Uploads the transformed files to S3.
Logs each step of the process.

2. ETL.py
Objective: Downloads a CSV file from an S3 bucket, reads it into a Pandas DataFrame, and uploads it into a MySQL database.

Key Features:
Downloads CSV from S3.
Creates a MySQL table if it doesn't already exist.
Inserts the data from the CSV into the MySQL table.

Setup:

Prerequisites
Python 3.x
AWS Account and S3 Bucket
MySQL Database (RDS or local)
Required Python Libraries (can be installed using requirements.txt)

Installation
Clone the repository:

git clone https://github.com/AakashRavindran/DE_Enhanced-ETL-Workflow-with-Python-AWS-S3-RDS-and-Glue.git
cd etl-pipeline


Install the required Python dependencies :
pip install -r requirements.txt

Configure your AWS credentials (using AWS CLI or by setting environment variables). Ensure that the following permissions are granted:

Read/Write access to S3 bucket
Permissions to access the RDS database (if using MySQL)
Update configuration values in both Python scripts:

S3 Bucket Name: Set your S3 bucket name in both main.py and ETL.py.
MySQL Database Connection: Update the RDS_HOST, RDS_USER, RDS_PASSWORD, and TABLE_NAME in ETL.py.


AWS Setup
Ensure that your AWS environment has access to the S3 bucket and that the AWS SDK (Boto3) is properly configured with your credentials.

MySQL Setup
If using RDS or a MySQL instance, make sure that the database is accessible from the machine running the script and that the appropriate tables are created dynamically by the script.

Usage
1. Running the File Transformation Process (main.py)
This script transforms local CSV, JSON, and XML files and uploads them to the specified S3 bucket.

To run the script, simply execute:

python3 main.py

The script will look for files in the guviS3 folder (this path is configurable).
The transformed files will be saved in the transformed folder, and then uploaded to S3.
A log file (process_log.txt) will be created with detailed information about the process.


2. Running the ETL Process (ETL.py)
This script will download a CSV file from an S3 bucket and insert the data into a MySQL database.

To run the script, execute:

python3 ETL.py

The script will download the specified CSV file from the S3 bucket and save it locally.
It will then read the CSV into a Pandas DataFrame.
The data will be inserted into a MySQL table.

S3 Bucket Structure
The project expects files to be uploaded to and downloaded from an S3 bucket. The transformed files are placed in a folder specified by s3_folder.

Files

main.py
Transforms CSV, JSON, and XML files.
Converts height from inches to meters, and weight from pounds to kilograms.
Uploads the transformed files to AWS S3.
Logs all transformations and uploads to process_log.txt.

ETL.py
Downloads CSV files from S3.
Reads the CSV into a Pandas DataFrame.
Creates the MySQL table dynamically based on the DataFrame columns (if the table doesn't already exist).
Inserts the data into MySQL.



