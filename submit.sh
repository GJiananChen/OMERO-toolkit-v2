#!/bin/bash

#SBATCH --job-name=omero_download
#SBATCH --output=logs/omero_download_%A_%a.out  # Output file for each job
#SBATCH --error=logs/omero_download_%A_%a.err   # Error file for each job
#SBATCH --time=12:00:00                    # Maximum execution time (hh:mm:ss)
#SBATCH --mem=4G                           # Memory per job
#SBATCH --cpus-per-task=4                  # Number of CPUs per job
#SBATCH --array=1-9                        # Array range based on the number of CSV files (adjust as needed)

CSV_DIR="omero_2566_wsi_chunks"
CSV_FILES=($CSV_DIR/*.csv)
CSV_FILE=${CSV_FILES[$SLURM_ARRAY_TASK_ID-1]}

python download_from_omero.py --csv_file "$CSV_FILE"
