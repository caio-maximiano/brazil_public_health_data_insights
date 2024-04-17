import os
import wget
import logging

class DataDownloader:
    """Class for downloading data using wget."""
    def download_data(self, url, download_path):
        """Download data using wget to a specified path, only if it does not already exist."""
        try:
            if not os.path.exists(download_path):
                os.makedirs(download_path, exist_ok=True)
            file_path = os.path.join(download_path, os.path.basename(url))
            if os.path.exists(file_path):
                logging.info(f"File already exists: {file_path}")
                return file_path
            wget.download(url, out=file_path)
            logging.info(f"Downloaded {url} to {file_path}")
            return file_path
        except Exception as e:
            logging.error(f"Failed to download data from {url}. Error: {e}")
            return None

