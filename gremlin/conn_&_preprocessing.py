import azure.cosmos.cosmos_client as cosmos_client
from azure.storage.blob import BlobServiceClient
from gremlin_python.driver import serializer
from dotenv import load_dotenv
import pandas as pd
import os 

# Loading the azure cosmos db variables from .env file
load_dotenv()

# Connection to Azure Cosmos DB
def get_cosmos_db_client():
    ENDPOINT = os.getenv('ENDPOINT')
    PRIMARY_KEY = os.getenv('PRIMARY_KEY')
    DATABASE = os.getenv('DATABASE')
    GRAPH = os.getenv('GRAPH')
    username = f"/dbs/{DATABASE}/colls/{GRAPH}"
    message_serializer = serializer.GraphSONSerializersV2d0()
    return cosmos_client.Client(ENDPOINT, username=username, password=PRIMARY_KEY, message_serializer=message_serializer)

# Connection to Azure Blob Storage
def get_blob_service_client():
    connection_string = os.getenv('CONNECTION_STRING')
    return BlobServiceClient.from_connection_string(connection_string)

# Function to read data from Azure Blob Storage
def read_blob_data(container_name, blob_name):
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob()
    return blob_data.readall().decode('utf-8')

# Function to preprocess the downloaded CSV files
def preprocess_csv_files():
    # Download CSV files and convert to DataFrames
    company_data = read_blob_data(container_name='layoffdata', blob_name='company_table.csv')
    company_df = pd.read_csv(pd.compat.StringIO(company_data))

    person_data = read_blob_data(container_name='layoffdata', blob_name='person_table.csv')
    person_df = pd.read_csv(pd.compat.StringIO(person_data))

    stock_data = read_blob_data(container_name='stockdata', blob_name='stock_table.csv')
    stock_df = pd.read_csv(pd.compat.StringIO(stock_data))

    layoff_data = read_blob_data(container_name='layoffdata', blob_name='layoff_table.csv')
    layoff_df = pd.read_csv(pd.compat.StringIO(layoff_data))

    region_data = read_blob_data(container_name='layoffdata', blob_name='region_table.csv')
    region_df = pd.read_csv(pd.compat.StringIO(region_data))

    industry_data = read_blob_data(container_name='layoffdata', blob_name='industry_table.csv')
    industry_df = pd.read_csv(pd.compat.StringIO(industry_data))

    # Define columns to keep for each DataFrame
    company_columns_to_keep = ["company_id", "company_name"]
    person_columns_to_keep = ["person_id", "person_name"]
    stock_columns_to_keep = ["CUSIP", "market_cap"]
    layoff_columns_to_keep = ["layoff_id", "layoff_type"]
    region_columns_to_keep = ["region_id", "region_name"]
    industry_columns_to_keep = ["industry_id", "industry_type"]

    # Drop unnecessary columns
    company_df = company_df[company_columns_to_keep]
    person_df = person_df[person_columns_to_keep]
    stock_df = stock_df[stock_columns_to_keep]
    layoff_df = layoff_df[layoff_columns_to_keep]
    region_df = region_df[region_columns_to_keep]
    industry_df = industry_df[industry_columns_to_keep]

    # Return DataFrames along with their labels and keys
    data = [
        {"df": company_df, "v_label": "company", "v_keys": company_columns_to_keep},
        {"df": person_df, "v_label": "person", "v_keys": person_columns_to_keep},
        {"df": stock_df, "v_label": "stock", "v_keys": stock_columns_to_keep},
        {"df": layoff_df, "v_label": "layoff", "v_keys": layoff_columns_to_keep},
        {"df": region_df, "v_label": "region", "v_keys": region_columns_to_keep},
        {"df": industry_df, "v_label": "industry", "v_keys": industry_columns_to_keep}
    ]
    
    return data

# Now call preprocess_csv_files() to get the preprocessed DataFrames
preprocessed_data = preprocess_csv_files()