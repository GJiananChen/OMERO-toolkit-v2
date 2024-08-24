# OMERO-toolkit
Simple but useful Python API code for common OMERO operations

Downloading many WSI from OMERO in an automated manner:

1. `chunk_omero_dataset.py` - Enumerate WSI present in a dataset stored in OMERO. The intention is to take the list of WSI and chunk the list into multiple csvs. These csvs can then be used to download the WSI in batches.
2. `download_from_omero.py` - Using the chunked list of WSI, download them. This can be parallelised further by using a slurm job (4).
3. `upload_dataset_omero.py` - Upload many WSI into a new OMERO project. 
4. `submit.sh` - Submit an array job that enumerates over all chunked csvs. Each job downloading total_wsi/chunk_size.

We also sometimes want to include the OMERO URL to browse to a given WSI in a metadata table. To automate the extraction of these URLs one can run:
`generate_browsable_omero_links.py`


Example use:

Setup conda and install requirements:

```bash
conda create --name omero
```

```bash
pip install -r requirements.txt
```

```python
python omero-toolkit/upload_dataset_omero.py --config configs/config.yaml --directory directory/to/wsi/ --threads 8
```
