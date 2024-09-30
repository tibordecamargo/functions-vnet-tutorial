import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
import logging


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        # Get the connection string from app settings
        connect_str = os.environ["AZURE_STORAGEBLOB_CONNECTIONSTRING"]
        logging.info("Connection string retrieved successfully.")

        # Create a BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        logging.info("BlobServiceClient created successfully.")

        # Specify the container name
        container_name = "dvc"

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(container_name)
        logging.info(f"Container client created for container: {container_name}")

        # List blobs in the container
        blob_list = list(container_client.list_blobs())
        logging.info(f"Retrieved {len(blob_list)} blobs from the container.")

        if not blob_list:
            return func.HttpResponse("No blobs found in the container.", status_code=200)

        # Create a string with blob names
        blob_names = "\n".join([blob.name for blob in blob_list])

        return func.HttpResponse(f"Blobs in the container:\n{blob_names}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
