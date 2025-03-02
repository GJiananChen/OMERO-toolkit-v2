import os
import omero
from omero.gateway import BlitzGateway
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import argparse
import yaml

# Set up argument parsing
parser = argparse.ArgumentParser(description="Download WSI files from OMERO server.")
parser.add_argument('--csv_file', type=str, help='CSV file with the chunk of WSI names', default=None)
parser.add_argument('--config', type=str, default='configs/config.yaml', help='Path to the YAML configuration file')

args = parser.parse_args()

# Load configuration from YAML file
with open(args.config, 'r') as file:
    config = yaml.safe_load(file)

omero_config = config['omero']

# Extract credentials and server details
host = omero_config['host']
port = omero_config['port']
username = omero_config['username']
password = omero_config['password']
dataset_ids = omero_config['dataset_id']

dataset_ids = [dataset_ids] if isinstance(dataset_ids, int) else dataset_ids

# Retrieve filenames from YAML if no CSV is provided
if args.csv_file:
    df = pd.read_csv(args.csv_file)
    wsi_names = df['WSI Names'].tolist()
else:
    wsi_names = omero_config.get('filenames', [])
    if not wsi_names:
        raise ValueError("No WSI filenames found in YAML config and no CSV file provided.")

def download_file(orig_file, save_dir):
    file_name = orig_file.getName()
    file_size = orig_file.getSize()
    file_path = os.path.join(save_dir, file_name)

    if os.path.exists(file_path) and os.path.getsize(file_path) == file_size:
        print(f"File {file_name} already exists and matches the size on server. Skipping download.")
        return
    
    print(f"Downloading {file_name} ({file_size} bytes)...")
    with open(file_path, 'wb') as f:
        for chunk in orig_file.getFileInChunks():
            f.write(chunk)
    print(f"File {file_name} downloaded successfully!")

# Connect to OMERO server
conn = BlitzGateway(username, password, host=host, port=port)
connected = conn.connect()

if connected:
    print("Connected to OMERO server successfully!")

    for dataset_id in dataset_ids:
        print(f"Processing dataset ID: {dataset_id}")
        dataset = conn.getObject("Dataset", dataset_id)
        
        if dataset:
            save_dir = f"{dataset.getName()}_ndpi_files"
            os.makedirs(save_dir, exist_ok=True)
            
            download_tasks = []
            with ThreadPoolExecutor() as executor:
                for image in dataset.listChildren():
                    if image.getName() in wsi_names:
                        print(f"Finding original NDPI file for image: {image.getName()} (ID: {image.getId()})")
                        fileset = image.getFileset()
                        if fileset is not None:
                            for orig_file in fileset.listFiles():
                                future = executor.submit(download_file, orig_file, save_dir)
                                download_tasks.append(future)
                        else:
                            print(f"No fileset found for image: {image.getName()}")
            
            for future in download_tasks:
                future.result()
        else:
            print(f"Dataset with ID {dataset_id} not found.")
    
    conn.close()
else:
    print("Failed to connect to OMERO server.")
