import pandas as pd
import os
import omero
from omero.gateway import BlitzGateway
from math import ceil
import yaml

# Load configuration from YAML file
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

omero_config = config['omero']

# Extract configuration details from the YAML file
username = omero_config['username']
password = omero_config['password']
host = omero_config['host']
port = omero_config['port']
dataset_id = omero_config['dataset_id']
chunk_size = omero_config['chunk_size']

# Function to chunk list into CSV files
def chunk_list(data_list, chunk_size):
    return [data_list[i:i + chunk_size] for i in range(0, len(data_list), chunk_size)]

# Connect to the OMERO server
conn = BlitzGateway(username, password, host=host, port=port)
connected = conn.connect()

if connected:
    print("Connected to OMERO server successfully!")
    
    dataset = conn.getObject("Dataset", dataset_id)
    
    if dataset:
        # Get all WSI names and filter out the macro images
        wsi_names = [image.getName() for image in dataset.listChildren() if "[0]" in image.getName()]
        
        # Chunk the WSI names into smaller lists
        chunks = chunk_list(wsi_names, chunk_size)
        
        # Create a directory to store the CSV files
        output_dir = f"omero_{dataset_id}_wsi_chunks"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save each chunk to a separate CSV file in the created directory
        for i, chunk in enumerate(chunks):
            df = pd.DataFrame(chunk, columns=['WSI Names'])
            csv_filename = os.path.join(output_dir, f"wsi_chunk_{i + 1}.csv")
            df.to_csv(csv_filename, index=False)
            print(f"Saved {len(chunk)} WSI names to {csv_filename}")
    
    else:
        print(f"Dataset with ID {dataset_id} not found.")
    
    conn.close()
else:
    print("Failed to connect to OMERO server.")
