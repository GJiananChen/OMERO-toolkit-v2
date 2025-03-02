# OMERO-toolkit
Simple but useful Python API code for common OMERO operations

Downloading, uploading or retrieving URLs for slides stored in OMERO in an automated manner:

1. `chunk_omero_dataset.py` - Enumerate WSI present in a dataset stored in OMERO. The intention is to take the list of WSI and chunk the list into multiple csvs. These csvs can then be used to download the WSI in batches.
2. `download_from_omero.py` - Using the chunked list of WSI, download them. This can be parallelised further by using a slurm job (see sbatch scripts).
3. `upload_dataset_omero.py` - Upload many WSI into a new OMERO project.
4. `generate_browsable_omero_links.py` - We also sometimes want to include the WSI URL for OMERO in a metadata table to enable a user to browse to a specific WSI.



Example use:

Setup conda and install requirements:

```bash
conda create -n omero python=3.9
```

```bash
pip install -r requirements.txt
```

```python
python omero-toolkit/upload_dataset_omero.py --config configs/config.yaml --directory directory/to/wsi/ --threads 8
```
