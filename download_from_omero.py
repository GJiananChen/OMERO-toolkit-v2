import os
import omero
from omero.gateway import BlitzGateway
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import argparse
import yaml

# Set up argument parsing
parser = argparse.ArgumentParser(description="Download WSI files from OMERO server based on a list in a CSV file.")
parser.add_argument('--csv_file', type=str, help='CSV file with the chunk of WSI names')
parser.add_argument('--config', type=str, default='configs/config.yaml', help='Path to the YAML configuration file')

args = parser.parse_args()

# Load configuration from YAML file
with open(args.config, 'r') as file:
    config = yaml.safe_load(file)

omero_config = config['omero']

# Replace with your credentials and server details from YAML
host = omero_config['host']
port = omero_config['port']
username = omero_config['username']
password = omero_config['password']
dataset_id = omero_config['dataset_id']

# Function to download files
def download_file(orig_file, save_dir):
    file_name = orig_file.getName()
    file_size = orig_file.getSize()
    file_path = os.path.join(save_dir, file_name)

    # Check if file already exists and its size matches
    if os.path.exists(file_path):
        local_file_size = os.path.getsize(file_path)
        if local_file_size == file_size:
            print(f"File {file_name} already exists and matches the size on server. Skipping download.")
            return
        else:
            print(f"File {file_name} exists but is incomplete or corrupted. Re-downloading.")

    print(f"Downloading {file_name} ({file_size} bytes)...")

    # Download the file
    with open(file_path, 'wb') as f:
        for chunk in orig_file.getFileInChunks():
            f.write(chunk)

    print(f"File {file_name} downloaded successfully!")

# Connect to the OMERO server
conn = BlitzGateway(username, password, host=host, port=port)
connected = conn.connect()

if connected:
    print("Connected to OMERO server successfully!")
    
    dataset = conn.getObject("Dataset", dataset_id)
    
    if dataset:
        # Read the CSV file to get the list of WSI names
        df = pd.read_csv(args.csv_file)
        wsi_names = df['WSI Names'].tolist()
        
        # Create a directory to save NDPI files
        save_dir = f"{dataset.getName()}_ndpi_files"
        os.makedirs(save_dir, exist_ok=True)
        
        download_tasks = []
        
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor() as executor:
            for image in dataset.listChildren():
                if image.getName() in wsi_names:
                    print(f"Finding original NDPI file for image: {image.getName()} (ID: {image.getId()})")
                    
                    # Get the fileset associated with the image
                    fileset = image.getFileset()
                    if fileset is not None:
                        for orig_file in fileset.listFiles():
                            # Submit the download task to the executor
                            future = executor.submit(download_file, orig_file, save_dir)
                            download_tasks.append(future)
                    else:
                        print(f"No fileset found for image: {image.getName()}")
        
        # Wait for all downloads to complete
        for future in download_tasks:
            future.result()

    else:
        print(f"Dataset with ID {dataset_id} not found.")
    
    # Close connection
    conn.close()
else:
    print("Failed to connect to OMERO server.")
