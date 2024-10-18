import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER = os.getenv("SOURCE_CONTAINER")  # Use the source container

# Initialize Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

def generate_directory_tree(container_name, output_file="blob_tree.txt"):
    """
    Generate a directory tree of the blob storage container and save it to a text file.
    """
    container_client = blob_service_client.get_container_client(container_name)

    directory_tree = {}
    
    # List all blobs in the container and build a nested dictionary for the tree
    for blob in container_client.list_blobs():
        parts = blob.name.split('/')
        current_level = directory_tree
        for part in parts:
            current_level = current_level.setdefault(part, {})

    # Write the directory tree to a text file
    with open(output_file, 'w') as f:
        def write_tree(level, indent=0):
            for key, sub_tree in level.items():
                f.write(' ' * indent + key + '/\n')
                write_tree(sub_tree, indent + 2)

        write_tree(directory_tree)
    
    print(f"[INFO] Directory tree saved to {output_file}")

if __name__ == "__main__":
    generate_directory_tree(CONTAINER)
