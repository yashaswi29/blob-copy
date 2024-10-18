import os
import time
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment variables
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
SOURCE_CONTAINER = os.getenv("SOURCE_CONTAINER")
SOURCE_FILE = os.getenv("SOURCE_FILE")

# Initialize Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)

def list_blobs_in_container(container):
    """List all blob paths in the container."""
    return [blob.name for blob in container.list_blobs()]

def search_and_copy_file(src_path, target_folder, all_blobs):
    """Find the target folder and copy the file to it."""
    base_folder = src_path.split('/')[0]  # Extract the language base folder (e.g., fre-mczbv-78d)
    
    # Check if the target folder is within the same base folder
    matching_paths = [blob for blob in all_blobs if blob.startswith(f"{base_folder}/{target_folder}/")]

    if matching_paths:
        # Construct the destination path and copy the file
        dest_path = f"{matching_paths[0]}/{os.path.basename(src_path)}"
        copy_blob(SOURCE_CONTAINER, src_path, dest_path)
    else:
        print(f"[ERROR] Target folder '{target_folder}' not found.")

def copy_blob(container_name, src_path, dest_path):
    """Copy the blob to the destination path."""
    print(f"[INFO] Copying '{src_path}' to '{dest_path}'...")

    src_blob = blob_service_client.get_blob_client(container_name, src_path)
    dest_blob = blob_service_client.get_blob_client(container_name, dest_path)

    # Start copy operation using the source URL
    copy_operation = dest_blob.start_copy_from_url(src_blob.url)

    # Wait for the copy to complete
    while copy_operation["copy_status"] in ["pending", "in_progress"]:
        print("[INFO] Copy in progress... Waiting...")
        time.sleep(2)
        copy_operation = dest_blob.get_blob_properties().copy

    if copy_operation["copy_status"] == "success":
        print(f"[SUCCESS] Copied to '{dest_path}'")
    else:
        print(f"[ERROR] Failed to copy '{src_path}'.")

def process_source_file(file_path, all_blobs):
    """Read the source.txt file and process each line."""
    try:
        with open(file_path, 'r') as file:
            for line in file:
                if line.strip():
                    src_path, target_folder = line.split(" : ")
                    search_and_copy_file(src_path.strip(), target_folder.strip(), all_blobs)
    except FileNotFoundError:
        print(f"[ERROR] Source file '{file_path}' not found.")
    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    # List all blobs in the source container
    blobs = list_blobs_in_container(container_client)

    # Process the source.txt file
    process_source_file(SOURCE_FILE, blobs)
