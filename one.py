import os
import time
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from urllib.parse import quote

# Load environment variables
load_dotenv()

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
SOURCE_CONTAINER = os.getenv("SOURCE_CONTAINER")
DESTINATION_CONTAINER = os.getenv("DESTINATION_CONTAINER")
SOURCE_FILE = os.getenv("SOURCE_FILE")

# Initialize Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

def encode_path(path):
    return quote(path, safe="/")

def log_info(message):
    print(f"[INFO] {message}")

def log_verify(label, status):
    icon = "✔" if status else "✖"
    print(f"[VERIFY] {label}: {icon} {'Found' if status else 'Not Found'}")

def log_status(status):
    icon = "✔" if status == "success" else "✖"
    print(f"[STATUS] Copy Operation: {icon} {status.capitalize()}")

def copy_blob(source_container, destination_container, src_path, dest_path):
    encoded_src_path = encode_path(src_path)
    source_blob_url = (
        f"https://{blob_service_client.account_name}.blob.core.windows.net/"
        f"{source_container}/{encoded_src_path}"
    )

    log_info(f"Copying Blob:\n  Source:      {src_path}\n  Destination: {dest_path}")

    try:
        blob_client = blob_service_client.get_blob_client(
            container=destination_container, blob=dest_path
        )
        copy_operation = blob_client.start_copy_from_url(source_blob_url)

        # Polling until copy operation is complete
        while copy_operation["copy_status"] in ["pending", "in_progress"]:
            print("  [INFO] Copy in progress... Waiting...")
            time.sleep(2)
            copy_operation = blob_client.get_blob_properties().copy

        log_status(copy_operation["copy_status"])

        # Verify source and destination paths
        verify_source(source_container, src_path)
        verify_destination(destination_container, dest_path)

    except Exception as e:
        print(f"[ERROR] Error copying '{src_path}' to '{dest_path}': {e}")

def verify_source(container, path):
    try:
        blob_client = blob_service_client.get_blob_client(container=container, blob=path)
        blob_client.get_blob_properties()
        log_verify(f"Source in '{container}'", True)
    except Exception:
        log_verify(f"Source in '{container}'", False)

def verify_destination(container, path):
    try:
        blob_client = blob_service_client.get_blob_client(container=container, blob=path)
        blob_client.get_blob_properties()
        print(f"[VERIFY] Destination:\n  {path} - ✔ Verified")
    except Exception:
        print(f"[VERIFY] Destination:\n  {path} - ✖ Not Found")

def process_files(source_file_path):
    try:
        with open(source_file_path, 'r') as src_file:
            for line in src_file:
                src_path, target = line.strip().split(" : ")
                target = target.strip('"')

                parts = src_path.strip("/").split("/")
                lang_id = parts[0]

                if target.startswith(lang_id):
                    # Case 1: Move within the same language directory to a different sub-folder
                    dest_path = f"{lang_id}/{target}/{parts[-1]}"
                else:
                    # Case 2: Move to a different language directory
                    dest_path = f"{target}/{'/'.join(parts[1:])}"

                copy_blob(SOURCE_CONTAINER, DESTINATION_CONTAINER, src_path, dest_path)

    except FileNotFoundError as e:
        print(f"[ERROR] Error: {e}")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")

if __name__ == "__main__":
    process_files(SOURCE_FILE)