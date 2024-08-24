import omero
from omero.gateway import BlitzGateway
import yaml
import argparse
import os
import subprocess
import time

parser = argparse.ArgumentParser(description="Upload a directory of WSI files to a new project on OMERO server")
parser.add_argument('--config', type=str, default='configs/config.yaml', help='Path to the YAML configuration file')
parser.add_argument('--directory', type=str, required=True, help='Path to the directory containing WSI files')
parser.add_argument('--threads', type=int, default=4, help='Number of parallel threads for uploading')

args = parser.parse_args()

# Load configuration from YAML file
with open(args.config, 'r') as file:
    config = yaml.safe_load(file)
omero_config = config['omero']

# Extract configuration details from the YAML file
username = omero_config['username']
password = omero_config['password']
host = omero_config['host']
port = omero_config['port']

# Get the project name from the configuration
project_name = omero_config.get('new_project_name', None)

if project_name is None:
    print("Project name must be provided in the config file.")
    exit(1)

# Function to upload a single WSI file as an image using the OMERO CLI importer
def upload_image(file_path, dataset_id):
    try:
        # Use the OMERO CLI import command to upload the image
        cmd = [
            "omero", "import",
            "-s", host,
            "-u", username,
            "-w", password,
            "-d", f"Dataset:{dataset_id}",
            file_path
        ]
        print(f"Running command: {' '.join(cmd)}")

        start_time = time.time()

        # Capture the CLI output
        result = subprocess.run(cmd, capture_output=True, text=True)

        end_time = time.time()
        duration = end_time - start_time
        file_size = os.path.getsize(file_path)
        upload_speed = file_size / duration / (1024 * 1024)  # Convert to MB/s

        if result.returncode == 0:
            print(f"{os.path.basename(file_path)} uploaded successfully.")
            print(f"Upload speed: {upload_speed:.2f} MB/s")
        else:
            print(f"Failed to upload {os.path.basename(file_path)}: {result.stderr}")
            return (file_path, "Failed")

        return (file_path, "Success")
    except subprocess.CalledProcessError as e:
        print(f"Failed to upload {os.path.basename(file_path)}: {e}")
        return (file_path, "Failed")

# Connect to the OMERO server
conn = BlitzGateway(username, password, host=host, port=port)
connected = conn.connect()

if connected:
    print("Connected to OMERO server successfully!")
    
    # Get the directory containing WSI files
    directory = args.directory
    if not os.path.isdir(directory):
        print(f"Directory {directory} does not exist.")
        conn.close()
        exit(1)

    # Create a new project
    new_project = omero.model.ProjectI()
    new_project.setName(omero.rtypes.rstring(project_name))
    saved_project = conn.getUpdateService().saveAndReturnObject(new_project)

    # Check if the project was created successfully
    if saved_project is None or saved_project.id is None:
        print("Failed to create the project.")
        conn.close()
        exit(1)

    project_id = saved_project.id.val
    print(f"Project '{project_name}' created with ID {project_id}")

    # Create a single dataset for all the WSI files
    dataset_name = project_name + "_Dataset"
    new_dataset = omero.model.DatasetI()
    new_dataset.setName(omero.rtypes.rstring(dataset_name))
    new_dataset = conn.getUpdateService().saveAndReturnObject(new_dataset)

    # Link the dataset to the project
    link = omero.model.ProjectDatasetLinkI()
    link.setParent(omero.model.ProjectI(project_id, False))
    link.setChild(omero.model.DatasetI(new_dataset.id.val, False))
    conn.getUpdateService().saveObject(link)

    # Upload each WSI file as an image to the dataset
    wsi_files = [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    
    if not wsi_files:
        print(f"No WSI files found in directory {directory}.")
        conn.close()
        exit(1)

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [executor.submit(upload_image, wsi_file, new_dataset.id.val) for wsi_file in wsi_files]
        for future in futures:
            future.result()

    conn.close()
else:
    print("Failed to connect to OMERO server.")
