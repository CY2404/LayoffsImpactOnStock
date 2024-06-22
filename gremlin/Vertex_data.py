import threading
from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError
from dotenv import load_dotenv
import os
import sys
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)
from data.company_df import company_df
from data.stock_df import stock_df
from data.person_df import person_df
from data.industry_df import industry_df
from data.region_df import region_df
from data.layoff_df import layoff_df


# Loading the azure cosmos db variables from .env file
load_dotenv()

ENDPOINT = os.getenv('ENDPOINT')
PRIMARY_KEY = os.getenv('PRIMARY_KEY')
DATABASE = os.getenv('DATABASE')
GRAPH = os.getenv('GRAPH')

# Function for creating vertex
def create_vertex(client, label, properties):
    try:
        # Gremlin query 
        query = f"g.addV('{label}'){''.join(f'.property(\'{prop}\', {repr(value)})' for prop, value in properties.items())}"
        
        # Printing the constructed query for inspection
        print("Constructed query:", query)

        # Executing the query
        callback = client.submitAsync(query)
        results = callback.result()
        for result in results:
            print(result)
    except GremlinServerError as e:
        print(f"Error executing query: {e}")

# Function for vertex worker : to process each row in the dataframe
def vertex_worker(df, g_client, v_label, v_keys):
    for row in df:
        properties = {key: row[key] for key in v_keys}  
        create_vertex(g_client, v_label, properties)

# Function to insert vertices to client
def insert_vertices_to_client(df, g_client, v_label, v_keys):
    try:
        for row in df:
            properties = {key: row[key] for key in v_keys}  
            create_vertex(g_client, v_label, properties)
    except Exception as e:
        print(f"Error inserting vertices: {e}")

# Initializing Gremlin client
gremlin_client = client.Client(
    ENDPOINT,
    'g',
    username=f'/dbs/{DATABASE}/colls/{GRAPH}',
    password=PRIMARY_KEY,
    message_serializer=serializer.GraphSONSerializersV2d0()
)


# Dataframes
vertex_dfs = [
    {
        "df": company_df,
        "v_label": "company",
        "v_keys": ["company_id", "company_name", "ids"]
    },
    {
        "df": person_df,
        "v_label": "person",
        "v_keys": ["person_id", "person_name", "ids"]
    },
    {
        "df": stock_df,
        "v_label": "stock",
        "v_keys": ["CUSIP", "market_cap", "ids"]
    },
    {
        "df": layoff_df,
        "v_label": "layoff",
        "v_keys": ["layoff_id", "layoff_type", "ids"]
    },
    {
        "df": region_df,
        "v_label": "region",
        "v_keys": ["region_id", "region_name", "ids"]
    },
    {
        "df": industry_df,
        "v_label": "industry",
        "v_keys": ["industry_id", "industry_type", "ids"]
    }
]

# Inserting vertices using threading
threads = []
# Iterate over vertex_dfs and create vertices
for vertex_info in vertex_dfs:
    df = vertex_info["df"]
    v_label = vertex_info["v_label"]
    v_keys = vertex_info["v_keys"]
    insert_vertices_to_client(df, gremlin_client, v_label, v_keys)

# Waiting for all threads to complete
for t in threads:
    t.join()

# Close the connection afterwards
gremlin_client.close()
