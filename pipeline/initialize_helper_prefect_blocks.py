from prefect_gcp.credentials import GcpCredentials
from prefect.logging import get_run_logger
import variables as pvars


logging = get_run_logger()


def create_gcp_cred_block():
    credentials = GcpCredentials(
    service_account_file=pvars.SERVICE_ACC_PATH).save("gcp-key")
    logging.info("Created <gcp-key> GcpCredentials block")


if __name__ == "__main__":
    create_gcp_cred_block()