import omero
from omero.gateway import BlitzGateway

# Replace with your credentials and server details
username = 'USERNAME_HERE'
password = 'PASSWORD_HERE'
host = 'ucl-hist-omero.cs.ucl.ac.uk'
port = 4064

# Replace with your OMERO.web base URL
web_base_url = 'https://ucl-hist-omero.cs.ucl.ac.uk/webclient/img_detail/'

# List of filenames
filenames = [
    '67_20-873_X_L1-3_X_b16_X',
    '270_19-517_B_L1-3_X_b2_X',
]

filenames = [f"{name}.ndpi [0]" for name in filenames]

# Specify the project or dataset ID you want to search within
project_id = 251 
dataset_id = None  # Set to None if you don't want to restrict by dataset

# Function to generate browseable links
def generate_links(conn, filenames, web_base_url, project_id=None, dataset_id=None):
    links = []
    
    # Optionally restrict the search to a specific project or dataset
    search_context = None
    if dataset_id:
        search_context = conn.getObject("Dataset", dataset_id)
    elif project_id:
        search_context = conn.getObject("Project", project_id)
    
    for filename in filenames:
        image = None
        if search_context:
            if dataset_id:
                images = search_context.listChildren()  # Search within a dataset
            else:
                datasets = search_context.listChildren()  # Search within all datasets of a project
                images = (img for ds in datasets for img in ds.listChildren())
            
            for img in images:
                if img.getName() == filename:
                    image = img
                    break
        else:
            # Search across all accessible images
            image = conn.getObject("Image", attributes={"name": filename})

        if image is not None:
            image_id = image.getId()
            # Construct the URL using the image ID
            browseable_link = f"{web_base_url}{image_id}/"
            links.append((filename, browseable_link))
            print(browseable_link)
        else:
            print(f"File {filename} not found on the OMERO server.")
            links.append((filename, None))
    
    return links


# Connect to the OMERO server
conn = BlitzGateway(username, password, host=host, port=port)
connected = conn.connect()

if connected:
    print("Connected to OMERO server successfully!")
    generate_links(conn, filenames, web_base_url, project_id=project_id, dataset_id=dataset_id)
    conn.close()
else:
    print("Failed to connect to OMERO server.")
