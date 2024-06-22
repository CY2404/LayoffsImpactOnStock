import threading
from gremlin_python.driver import client, serializer
import time
from gremlin_python.driver import protocol
from gremlin_python.driver.protocol import GremlinServerError
from dotenv import load_dotenv
import os
import sys
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)
from data.Affects_df import Affects_df
from data.BelongIn_df import BelongIn_df
from data.Employs_df import Employs_df
from data.Experiences_df import Experiences_df
from data.Suffers_df import Suffers_df
from data.FallsUnder_df import FallsUnder_df
from data.Issues_df import Issues_df

# Loading the azure cosmos db variables from .env file
load_dotenv()

ENDPOINT = os.getenv('ENDPOINT')
PRIMARY_KEY = os.getenv('PRIMARY_KEY')
DATABASE = os.getenv('DATABASE')
GRAPH = os.getenv('GRAPH')

# Initialize Gremlin client
gremlin_client = client.Client(
    ENDPOINT,
    'g',
    username=f'/dbs/{DATABASE}/colls/{GRAPH}',
    password=PRIMARY_KEY,
    message_serializer=serializer.GraphSONSerializersV2d0()
)

def create_gremlin_query(df, from_v_key, to_v_key, e_label):
    queries = []
    for index, row in df.iterrows():
        source = row[from_v_key]
        dest = row[to_v_key]
        query = f"g.V('{source}').addE('{e_label}').to(g.V('{dest}'))"
        print("Constructed edge query:", query)  # Print the constructed query
        queries.append(query)
    return queries


# Inside the insert_edges function
def insert_edges(client, all_edges):
    for idx, query in enumerate(all_edges):
        if idx % 800 == 0:
            time.sleep(3)
        try:
            client.submit(query)
        except protocol.GremlinServerError as e:
            print("Gremlin Server Error:", e.status_code)
        except Exception as e:
            print("An unexpected error occurred:", e)

# Inside the edge_worker function
def edge_worker(df, g_client, from_v_key, to_v_key):
    insert_edges(g_client, create_gremlin_query(df, edge_info["e_label"],from_v_key, to_v_key))


# Define the edge dataframes
edge_dfs = [
    {
        "df": BelongIn_df,
        "e_label": "BelongIn",
        "from_v_key": "company_id",
        "to_v_key": "region_id"
    },
    {
        "df": Employs_df,
        "e_label": "Employs",
        "from_v_key": "company_id",
        "to_v_key": "person_id"
    },
    {
        "df": FallsUnder_df,
        "e_label": "FallsUnder",
        "from_v_key": "company_id",
        "to_v_key": "industry_id"
    },
    {
        "df": Issues_df,
        "e_label": "Issues",
        "from_v_key": "company_id",
        "to_v_key": "CUSIP"
    },
    {
        "df": Suffers_df,
        "e_label": "Suffers",
        "from_v_key": "person_id",
        "to_v_key": "layoff_id"
    },
    {
        "df": Affects_df,
        "e_label": "Affects",
        "from_v_key": "layoff_id",
        "to_v_key": "CUSIP"
    },
    {
        "df": Experiences_df,
        "e_label": "Experiences",
        "from_v_key": "company_id",
        "to_v_key": "layoff_id"
    }
]

threads = []
# Inside the main section where threads are created and started
for edge_info in edge_dfs:
    t = threading.Thread(target=edge_worker, args=(edge_info["df"], gremlin_client, edge_info["from_v_key"], edge_info["to_v_key"]))
    threads.append(t)
    t.start()

# Wait for all threads to complete
for t in threads:
    t.join()
