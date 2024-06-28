# Running In Production

### Requirements
Ensure that you have the following tools installed in your local machine:
1. **[Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)**
2. **[Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)**
<!-- 3. **[GitHub CLI (`gh`)](https://cli.github.com/manual/installation)** -->

### Access and Credentials
1. Obtain the following credentials:
    - **GCP Service Account** with the following permissions:
        - **BigQuery Admin**
        - **Storage Admin**
    - **Kaggle API Key**
    - **Astronomer Cloud**
    - A *Workspace-scoped* Author API Token
    - Some resource IDs:
        - **ORGANIZATION ID**
        - **WORKSPACE ID**

## Deployment Setup

1. **Clone the Repository**:
      ```sh
      git clone https://github.com/shakik19/transfermarkt-data-pipeline.git
      ```

2. **Set Up Credentials**:
   - Navigate to the [production infrastructure](../infrastructure/prod/) directory:
   - Copy the contents of `template.env` to a separate `.env` in the same directory.
   - Very carefully read and add all required variables in the `.env` and `*.tf` files  
   *[I could have configured a Secret Manager to securely store all the sensitive variables except the gcp connection but ultimately I would have to make them available in the environment because there is no other options to authenticate DBT with BigQuery and the Kaggle api]*

3. **Create the resources**:
  
    Run the following commands to create GCP and Astronomer resources
    ```bash
    terraform init
    source .env
    terraform plan
    terraform apply
    ```
    Check the outputs and see if the resources are created without error 

4. **Configure Astronomer Cloud**:
    - Go to [Astronomer Cloud](https://cloud.astronomer.io) and add a **GCP connection** with the following details:
      - **Connection ID**: `google_cloud_default` (case-sensitive)
    - Copy the **DEPLOYMENT ID** of the deployment you just created
5. **Add GitHub Actions Secrets**:
    - Add the following secrets to your GitHub repository under **Settings > Secrets and variables > Actions**:
      - `ASTRO_API_TOKEN`: The API token used previously
      - `PROD_DEPLOYMENT_ID`: The deployment ID

6. **Push the Repository to your GitHub account**

7. **Monitor GitHub Actions CI Job**:
 
    The GitHub Actions CI job will get triggered automatically. Monitor the job's progress in the **Actions** tab of your GitHub repository.

8. **Verify Deployment**:
    - Upon completion of the CI job, your Astronomer deployment should be visible and running.

9.  **Destroying the resources**:
      ```bash
      terraform -chdir=./infrastructure/prod/ destroy
      ```
      It still needs all env vars to be available.

### [Here](../assets/airflow/) are some images taken from my astronomer production environments 

## Additional Comments
- Ensure your `.env` file contains all the required variables with correct values.
- Monitor your deployment for any anomalies and troubleshoot as necessary.