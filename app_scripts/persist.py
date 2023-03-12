from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from pathlib import Path

##
# Saves files to Azure Blob Storage.
##
def save_to_azure(container_name, file_name, file_location):
    account_url = "https://movierecsdataset.blob.core.windows.net"
    default_credential = DefaultAzureCredential()

    blob_service_client = BlobServiceClient(account_url, credential=default_credential)

    if blob_service_client.get_container_client(container_name) is None:
        print("Creating new container...")
        # create container
        blob_service_client.create_container(container_name)

    blob_client =  blob_service_client.get_blob_client(container=container_name, blob=file_name)

    print("Uploading file to Azure storage as \n\t", file_name)

    with open(file=file_location, mode="rb") as data:
        blob_client.upload_blob(data, overwrite=True)


if __name__ == "__main__":
    ROOT = Path(__file__).parent.parent

    details_fileName = "details-5000.parquet"
    save_to_azure("datasets", details_fileName, f'{ROOT}/app_data/{details_fileName}')
    genres_fileName = "genre_list-5000.parquet"
    save_to_azure("datasets", genres_fileName, f'{ROOT}/app_data/{genres_fileName}')
