#!/bin/bash

export TF_VAR_service_account="/home/shakik/Documents/git-repos/transfermarkt-data-pipeline/include/keys/gcp_key.json"
export TF_VAR_project_id="$PROJECT_ID"
export TF_VAR_region="us-west1"
export TF_VAR_location="US"
export TF_VAR_bucket_name="$BUCKET_NAME"
export TF_VAR_seed_dataset_id="$SEED_DATASET_NAME"
export TF_VAR_core_dataset_id="$CORE_DATASET_NAME"
