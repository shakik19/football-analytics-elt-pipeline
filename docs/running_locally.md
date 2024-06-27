# Running Locally

### Requirements
Ensure that you have the following tools installed:
1. **[Docker](https://www.docker.com/get-started)**
2. **[Astronomer Astro CLI](https://docs.astronomer.io/astro/cli/overview)**
3. **[Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)**

### Access and Credentials
1. Obtain a GCP Service Account with minimum permissions to create and delete GCS buckets and BigQuery datasets:
    - **BigQuery Admin**
    - **Storage Admin**
2. Obtain a **Kaggle API** key.

## Development Setup

1. **GCP Service Account Key**:
    - Place the GCP service account JSON file in `./include/keys/` and rename it to `gcp_key.json`.

<!-- 2. **Kaggle API Credentials**:
    - Set the `KAGGLE_USERNAME` and `KAGGLE_KEY` environment variables at `./.env` using the values from your `kaggle.json` API token. -->

3. **Environment Variables**:
   - Copy `./template.env` & `./infrastructure/dev/template.env` to separate `.env` files in their respective locations.
   - Set all the values of the *.env* files

4. **Spinning up the pipeline**:
    
    - Check the `local_run.sh` script and run it to create all the necessary resources.
      ```bash
      chmod +x ./local_run.sh
      ./local_run.sh
      ```
    - To initialize an astro project and start it, run
      ```bash
      astro dev init
      astro dev start
      ```

5. **Access Airflow UI**:
    - Open your web browser and go to [http://localhost:8080](http://localhost:8080) to access the Airflow UI.

6. **Trigger the Pipeline**:
    - Use the Airflow UI to trigger your data pipeline.

7.  **Shutdown and Cleanup**:
    - To shutdown the containers, run:
      ```bash
      astro dev stop
      ```
    - To clean up the Terraform resources, run:
      ```bash
      terraform -chdir=./infrastructure/dev/ destroy
      ```
      It still needs all env vars to be available.

    - To clean up the project, run:
      ```bash
      astro dev kill
      ```

Follow these steps precisely to ensure your local environment is correctly set up and your data pipeline runs smoothly.

**NOTE:** In case you face error like *Zombie task* or *Zombie jobs* found, you need to set some additional Airflow settings through environment variable. Refer to the `./template.env` file and read commented error handaling description below.






