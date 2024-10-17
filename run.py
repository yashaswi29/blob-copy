import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv()

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
SOURCE_CONTAINER = os.getenv("SOURCE_CONTAINER")
DESTINATION_CONTAINER = os.getenv("DESTINATION_CONTAINER")
SOURCE_FILE = os.getenv("SOURCE_FILE")

blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

def encode_path(path):
    return quote(path, safe="/")

def copy_blob(source_container, destination_container, src_path, dest_path):
    encoded_src_path = encode_path(src_path)
    source_blob_url = (
        f"https://{blob_service_client.account_name}.blob.core.windows.net/"
        f"{source_container}/{encoded_src_path}"
    )
    print(f"Copying from: {source_blob_url} to {dest_path}")
    try:
        blob_client = blob_service_client.get_blob_client(
            container=destination_container, blob=dest_path )
        copy_operation = blob_client.start_copy_from_url(source_blob_url)
        print(f"Copy Status: {copy_operation['copy_status']}")
        verify_copy(dest_path)
    except Exception as e:
        print(f"Error copying '{src_path}' to '{dest_path}': {e}")

def verify_copy(dest_path):
    blob_client = blob_service_client.get_blob_client(
        container=DESTINATION_CONTAINER, blob=dest_path )
    try:
        blob_client.get_blob_properties()
        print(f"Verification Success: '{dest_path}' exists.")
    except Exception as e:
        print(f"Verification Failed: '{dest_path}' does not exist. Error: {e}")

def process_files(source_file_path):
    try:
        with open(source_file_path, 'r') as src_file:
            for line in src_file:
                src_path, lang_id = line.strip().split(" : ")
                lang_id = lang_id.strip('"')
                parts = src_path.strip("/").split("/")
                parts[0] = lang_id 
                dest_path = "/".join(parts)
                copy_blob(SOURCE_CONTAINER, DESTINATION_CONTAINER, src_path, dest_path)

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    process_files(SOURCE_FILE)
