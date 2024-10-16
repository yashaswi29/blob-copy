import os
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from urllib.parse import quote

# Load environment variables from .env
load_dotenv()

# Configuration from environment variables
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
SOURCE_CONTAINER = os.getenv("SOURCE_CONTAINER")
DESTINATION_CONTAINER = os.getenv("DESTINATION_CONTAINER")
BLOB_PREFIX = os.getenv("BLOB_PREFIX", "")  # Folder path where JSON files are stored

LANGUAGE_ID_MAP = {
    "ine-nhxrt-19q": "india",
    "hin-pqwrk-23h": "hindi",
    "fre-mczbv-78d": "french"
}

# Initialize Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

def encode_path(path):
    """URL-encode the blob path."""
    return quote(path, safe="/")

def copy_blob(src_container, dest_container, src_path, dest_path):
    """Copy a blob from source to destination."""
    encoded_src_path = encode_path(src_path)
    source_blob_url = (
        f"https://{blob_service_client.account_name}.blob.core.windows.net/"
        f"{src_container}/{encoded_src_path}"
    )

    print(f"Source Blob URL: {source_blob_url}")
    print(f"Destination Path: {dest_path}")

    try:
        blob_client = blob_service_client.get_blob_client(
            container=dest_container, blob=dest_path
        )
        copy_operation = blob_client.start_copy_from_url(source_blob_url)
        print(f"Copying '{src_path}' to '{dest_path}'. Status: {copy_operation['copy_status']}")
    except Exception as e:
        print(f"Error copying '{src_path}' to '{dest_path}': {e}")

def find_destination_path(src_path, lang_id):
    """Determine the correct destination path based on the language ID."""
    parts = src_path.split('/')
    asset_type, filename = parts[-2], parts[-1]

    language_folder = LANGUAGE_ID_MAP.get(lang_id)
    if not language_folder:
        raise ValueError(f"Unknown language ID: {lang_id}")

    return f"{language_folder}/{asset_type}/{filename}"

def process_json_from_blob(blob_name):
    """Read and process a JSON file from the blob storage."""
    try:
        blob_client = blob_service_client.get_blob_client(SOURCE_CONTAINER, blob_name)
        json_data = json.loads(blob_client.download_blob().readall())
        lang_id = json_data.get("langId")

        for asset_type in ["images", "videos", "documents"]:
            assets = json_data.get(asset_type, [])
            for src_path in assets:
                try:
                    dest_path = find_destination_path(src_path, lang_id)
                    copy_blob(SOURCE_CONTAINER, DESTINATION_CONTAINER, src_path, dest_path)
                except ValueError as e:
                    print(e)

    except Exception as e:
        print(f"Error processing '{blob_name}': {e}")

def traverse_and_process_json_files():
    """Traverse the blob storage to find and process all JSON files."""
    try:
        container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
        blob_list = container_client.list_blobs(name_starts_with=BLOB_PREFIX)

        for blob in blob_list:
            if blob.name.endswith(".json"):
                print(f"Processing JSON: {blob.name}")
                process_json_from_blob(blob.name)

    except Exception as e:
        print(f"Error traversing blob storage: {e}")

if __name__ == "__main__":
    traverse_and_process_json_files()
