#!/bin/bash
#SBATCH --job-name=upload_wsi            # Job name
#SBATCH --output=logs/upload_wsi_%j.log       # Output and error log file (%j expands to job ID)
#SBATCH --error=logs/upload_wsi_%j.err        # Error log file
#SBATCH --time=12:00:00                  # Time limit (hh:mm:ss)
#SBATCH --cpus-per-task=8                # Number of CPU cores per task
#SBATCH --mem=16G                        # Memory per node
#SBATCH --partition=cpuq             # Partition name

python upload_dataset_omero.py --config configs/config.yaml --directory /scratch/glastonbury/spatial_he/ --threads 8
