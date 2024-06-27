#!/bin/bash

# Function to source environment variables and handle errors
source_env_files() {
  local env_file="$1"
  if [[ ! -f "$env_file" ]]; then
    echo "Error: Environment file '$env_file' not found." >&2
    exit 1
  fi
  if ! source "$env_file"; then
    echo "Error: Failed to source environment file '$env_file'." >&2
    exit 1
  fi
  echo "Loaded environment variables from '$env_file'."
}

# Source environment variables
source_env_files ".env"
source_env_files "./infrastructure/dev/.env"

# Set Terraform working directory
tf_workdir="-chdir=./infrastructure/dev/"

# Run Terraform commands with error handling
if ! terraform $tf_workdir init; then
  echo "Error: Terraform initialization failed." >&2
  exit 1
fi
echo "Terraform initialization successful."

if ! terraform $tf_workdir apply; then
  echo "Error: Terraform apply failed." >&2
  exit 1
fi
echo "Terraform apply successful."
