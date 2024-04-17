import os
import logging
import time
from io import BytesIO
import zipfile
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set Azure SDK logging to WARNING to suppress detailed INFO logs
logging.getLogger('azure').setLevel(logging.WARNING)

# Decorator for handling Azure storage exceptions
def azure_storage_exception_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ResourceExistsError:
            logging.error(f"Blob already exists and overwrite is set to False.")
            return False
        except ResourceNotFoundError:
            logging.error("Container not found for blob.")
            return False
        except zipfile.BadZipFile:
            logging.error("The downloaded file is not a valid ZIP file.")
            return False
        except Exception as e:
            logging.error(f"Failed operation. Error: {e}")
            return False
    return wrapper

# Decorator for logging method execution details
def log_method_call(func):
    def wrapper(*args, **kwargs):
        logging.info(f"Entering {func.__name__}")
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"Exiting {func.__name__}. Duration: {end_time - start_time:.2f} seconds.")
        return result
    return wrapper

# Azure Blob Storage Handler Class
class AzureBlobStorageHandler:
    """Storage handler for Azure Blob Storage."""

    def __init__(self, connection_string, container_name):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)

    @azure_storage_exception_handler
    @log_method_call
    def save(self, file_path, blob_name):
        """Uploads a file to Azure Blob Storage."""
        if not os.path.isfile(file_path):
            logging.error(f"The file {file_path} does not exist.")
            return False
        blob_client = self.container_client.get_blob_client(blob_name)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True, max_concurrency=5)
            logging.info(f"Uploaded file to Azure Blob Storage as {blob_name}")
            return True

    @azure_storage_exception_handler
    @log_method_call
    def download_blob(self, blob_name, download_path):
        """Downloads a blob to a local file."""
        blob_client = self.container_client.get_blob_client(blob_name)
        with open(download_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
            logging.info(f"Downloaded blob {blob_name} to {download_path}")

    @azure_storage_exception_handler
    @log_method_call
    def extract_zip_in_blob(self, zip_blob_name, target_folder):
        """Extracts a ZIP file stored in a blob and uploads its contents back into blob storage."""
        blob_client = self.container_client.get_blob_client(zip_blob_name)
        zip_bytes = blob_client.download_blob().readall()
        
        logging.info(f"Downloaded zip blob size: {len(zip_bytes)} bytes")
        
        with BytesIO(zip_bytes) as zip_stream:
            with zipfile.ZipFile(zip_stream, 'r') as zip_file:
                for zip_info in zip_file.infolist():
                    if not zip_info.is_dir():
                        file_name = zip_info.filename
                        target_blob_name = os.path.join(target_folder, file_name)
                        with zip_file.open(zip_info) as file_data:
                            self.container_client.upload_blob(name=target_blob_name, data=file_data.read(), overwrite=True)
                            logging.info(f"Uploaded extracted file to {target_blob_name}")
