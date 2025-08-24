import csv
from azure.storage.blob import BlobServiceClient
import os
from logger import _logger

class AzureStorageReader:
    def __init__(self, connection_string, container_name, folder_name, output_file):
        self.connection_string = connection_string
        self.container_name = container_name
        self.folder_name = folder_name
        self.output_file = output_file
        self.logger = _logger('DEBUG', 'AzureStorageReader')
        self.blob_service_client = None

    def connect(self):
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            self.logger.info(f"Connected to Azure Storage account successfully.")
        except Exception as e:
            self.logger.error(f"Failed to connect to Azure Storage account: {e}")
            raise

    def list_files_in_folder(self):
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            self.logger.info(f"Accessing container: {self.container_name}")
            
            file_names = []
            blobs = container_client.list_blobs(name_starts_with=self.folder_name)
            for blob in blobs:
                file_names.append(blob.name)
                self.logger.info(f"Found file: {blob.name}")
            
            return file_names
        except Exception as e:
            self.logger.error(f"Failed to list files in folder: {e}")
            raise

    def write_file_names_to_output(self, file_names):
        try:
            with open(self.output_file, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(['File Name'])  # Write header
                for file_name in file_names:
                    csvwriter.writerow([file_name])
            self.logger.info(f"File names written to {self.output_file} as CSV successfully.")            
        except Exception as e:
            self.logger.error(f"Failed to write file names to output file: {e}")
            raise

    def process(self):
        try:
            self.connect()
            file_names = self.list_files_in_folder()
            self.write_file_names_to_output(file_names)
        except Exception as e:
            self.logger.error(f"Error during processing: {e}")
            raise

# Example usage
if __name__ == "__main__":
    qa_connection_string = "connection_string"
    container_name = "ib-sap"
    folder_name = "NSAP/PricingFivetran"
    output_file = "PricingFivetran.txt"

    reader = AzureStorageReader(qa_connection_string, container_name, folder_name, output_file)
    reader.process()