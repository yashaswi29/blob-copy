import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv()

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
SOURCE_CONTAINER = os.getenv("SOURCE_CONTAINER")
DESTINATION_CONTAINER = os.getenv("DESTINATION_CONTAINER")
SOURCE_FILE = os.getenv("SOURCE_FILE")
DESTINATION_FILE = os.getenv("DESTINATION_FILE")


blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
def encode_path(path):
    return quote(path, safe="/")

def copy_blob(source_container, destination_container, src_path, dest_path):
    encoded_src_path = encode_path(src_path)

    source_blob_url = (
        f"https://{blob_service_client.account_name}.blob.core.windows.net/"
        f"{source_container}/{encoded_src_path}"
    )
    print(f"Source Blob URL: {source_blob_url}")
    print(f"Destination Path: {dest_path}")

    try:
        blob_client = blob_service_client.get_blob_client(
            container=destination_container, blob=dest_path
        )
        copy_operation = blob_client.start_copy_from_url(source_blob_url)
       
        print(f"Copying '{src_path}' to '{dest_path}'. Status: {copy_operation['copy_status']}")
    except Exception as e:
        print(f"Error copying '{src_path}' to '{dest_path}': {e}")

def process_files(source_file_path, destination_file_path):
    try:
        with open(source_file_path, 'r') as src_file, open(destination_file_path, 'r') as dest_file:
            for src_line, dest_line in zip(src_file, dest_file):
                src_path = src_line.strip()
                dest_path = dest_line.strip()
                dest_full_path = os.path.join(dest_path, os.path.basename(src_path))

                copy_blob(SOURCE_CONTAINER, DESTINATION_CONTAINER, src_path, dest_full_path)

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    process_files(SOURCE_FILE, DESTINATION_FILE)
