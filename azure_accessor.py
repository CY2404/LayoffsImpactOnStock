from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

ACCOUNT_URL = "https://finalproject7275grp3.blob.core.windows.net/"
DEFAULT_CREDENTIAL = DefaultAzureCredential()

# Create the BlobServiceClient object
blob_service_client = BlobServiceClient(ACCOUNT_URL, credential=DEFAULT_CREDENTIAL)

def file_downloader(download_file_path: str, container_name: str, blob_name: str):
    """
        download_file_path: str, local file path
        container_name: str, container name i.e. stockdata, layoffdata
        blob_name: str, file in container name
    """
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(file=download_file_path, mode="wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    print(f"File at {download_file_path} has finished downloading")

def file_uploader(upload_file_path: str, container_name: str, blob_name: str):
    """
        upload_file_path: str, local file path
        container_name: str, container name i.e. stockdata, layoffdata
        blob_name: str, file to be named in container
    """
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(file=upload_file_path, mode="rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    print(f"File at {upload_file_path} has finished uploading")

