import json
import pandas as pd
import boto3
from lxml import etree
import os
import logging

# S3 configuration
BUCKET_NAME = r"guvi-project-glue-etl-aakash"
s3_folder = r"transformed_files"
s3_client = boto3.client('s3')

# Set up logging configuration
LOG_FILE = "process_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),  # Log to a file
        logging.StreamHandler()  # Also log to the console
    ]
)


# Function to convert inches to meters
def inch_to_m(inch):
    return inch * 0.0254


# Function to convert pounds to kilograms
def pounds_to_kg(pound):
    return pound * 0.453592


# Function to transform csv file
def transform_csv(file, out_folder):
    try:
        csv_data = pd.read_csv(file)
        csv_data.height = csv_data.height.apply(inch_to_m)
        csv_data.weight = csv_data.weight.apply(pounds_to_kg)
        original_file_name = os.path.splitext(os.path.basename(file))[0]
        new_file_name = f"{original_file_name}_transformed.csv"
        counter = 1
        output_file = os.path.join(out_folder, new_file_name)
        while os.path.exists(output_file):
            new_file_name = f"{original_file_name}_transformed_{counter}.csv"
            output_file = os.path.join(out_folder, new_file_name)
            counter += 1

        csv_data.to_csv(output_file, index=False)
        logging.info(f"CSV file {original_file_name} has been transformed and saved as {new_file_name}")
        return csv_data, new_file_name
    except Exception as e:
        logging.error(f"Error transforming CSV file {file}: {e}")


# Function to transform JSON data
def transform_json(file, out_folder):
    try:
        dict_json = []
        with open(file, "r") as json_file:
            for row in json_file.readlines():
                try:
                    dict_json.append(json.loads(row.strip()))
                except json.JSONDecodeError:
                    logging.warning(f"Skipping invalid line: {row.strip()}")

        df_json = pd.DataFrame(dict_json)
        df_json.height = df_json.height.apply(inch_to_m)
        df_json.weight = df_json.weight.apply(pounds_to_kg)

        original_file_name = os.path.splitext(os.path.basename(file))[0]
        new_file_name = f"{original_file_name}_transformed.csv"
        counter = 1
        output_file = os.path.join(out_folder, new_file_name)
        while os.path.exists(output_file):
            new_file_name = f"{original_file_name}_transformed_{counter}.csv"
            output_file = os.path.join(out_folder, new_file_name)
            counter += 1

        df_json.to_csv(output_file, index=False)
        logging.info(f"JSON file {original_file_name} has been transformed and saved as {new_file_name}")
        return df_json, new_file_name
    except Exception as e:
        logging.error(f"Error transforming JSON file {file}: {e}")


# Function to transform XML data
def transform_xml(file, out_folder):
    try:
        tree = etree.parse(file)
        root = tree.getroot()

        xml_data = []

        for person in root.findall("person"):
            name = person.find("name").text if person.find('name') is not None else 'Unknown'
            height = person.find("height").text if person.find('height') is not None else 0.0
            weight = person.find("weight").text if person.find('weight') is not None else 0.0
            xml_data.append({"name": name, "height": float(height), "weight": float(weight)})

        df_xml = pd.DataFrame(xml_data)

        df_xml.height = df_xml.height.apply(inch_to_m)
        df_xml.weight = df_xml.weight.apply(pounds_to_kg)

        original_file_name = os.path.splitext(os.path.basename(file))[0]
        new_file_name = f"{original_file_name}_transformed.csv"
        counter = 1
        output_file = os.path.join(out_folder, new_file_name)
        while os.path.exists(output_file):
            new_file_name = f"{original_file_name}_transformed_{counter}.csv"
            output_file = os.path.join(out_folder, new_file_name)
            counter += 1

        df_xml.to_csv(output_file, index=False)
        logging.info(f"XML file {original_file_name} has been transformed and saved as {new_file_name}")
        return df_xml, new_file_name
    except Exception as e:
        logging.error(f"Error transforming XML file {file}: {e}")


# Function to process all files in the folder
def process_files_in_folder(folder_path, output_folder):
    try:
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)

            # Skip directories and process only files
            if os.path.isdir(file_path):
                continue

            if file_name.endswith(".csv"):
                transform_csv(file_path, output_folder)
            elif file_name.endswith(".json"):
                transform_json(file_path, output_folder)
            elif file_name.endswith(".xml"):
                transform_xml(file_path, output_folder)
            else:
                logging.warning(f"Unknown file type found: {file_name}")
    except Exception as e:
        logging.error(f"Error processing files in folder {folder_path}: {e}")


# Function to upload a file to S3
def upload_to_s3(file_path):
    try:
        file_name = os.path.basename(file_path)
        s3_file_path = os.path.join(s3_folder, file_name).replace("\\", "/")  # Ensure correct path format for S3

        with open(file_path, 'rb') as data:
            s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_file_path, Body=data)

        logging.info(f"Uploaded {file_name} to s3://{BUCKET_NAME}/{s3_file_path}")
    except Exception as e:
        logging.error(f"Error uploading file {file_path} to S3: {e}")


# Function to upload all files in the transformed folder to S3
def upload_all_files_to_s3(transformed_folder):
    try:
        for file_name in os.listdir(transformed_folder):
            file_path = os.path.join(transformed_folder, file_name)

            if os.path.isdir(file_path):
                continue

            upload_to_s3(file_path)
    except Exception as e:
        logging.error(f"Error uploading files from {transformed_folder} to S3: {e}")


# Main execution
def main():
    # Process and transform files
    process_files_in_folder("guviS3", "transformed")

    # Upload all transformed files to S3
    upload_all_files_to_s3("transformed")

    # Finally, upload the log file to S3
    upload_to_s3(LOG_FILE)


# Run the main process
if __name__ == "__main__":
    main()
