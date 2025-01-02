import json
import time
import psycopg2
from psycopg2 import sql
import logging
import pandas as pd
from sqlalchemy import create_engine
from contextlib import contextmanager
from utils.custom_logger import CustomLogger

logger_class = CustomLogger('data_assets_count', level=logging.INFO)
logger = logger_class.setup_logger('data_asset')

def load_config():
    with open('db_config.json', 'r') as f:
        return json.load(f)

# Load database configurations
db_configs = load_config()

def create_connection_string(config, database_name):
    from urllib.parse import quote_plus
    return f"postgresql+psycopg2://{config['user']}:{quote_plus(config['password'])}@{config['server']}:{config['port']}/{database_name}"

# Create engines for target database
source_config = db_configs['192.168.3.243']
source_config['server'] = '192.168.3.243'
engine = create_engine(create_connection_string(source_config, 'data_assets'))

def safe_eval(x):
    try:
        return eval(x) if isinstance(x, str) else x
    except Exception:
        return None

# Fetch company mappings
def fetch_company_master(engine):
    query = "SELECT company_domain, source_table FROM da_new.test_company_master"
    mapping_df = pd.read_sql(query, engine)
    mapping_df['source_table'] = mapping_df['source_table'].apply(safe_eval)
    return {row['company_domain']: row['source_table'] for _, row in mapping_df.iterrows()}

company_master = fetch_company_master(engine)

# Fetch table mappings
def fetch_table_mappings(engine):
    query = """
        SELECT a.server_name, a.database_name, a.schema_name, a.table_name, a.column_name, company_master
        FROM da_new.test_tbl_source_mapping_master a WHERE company_master IS NOT NULL and a.table_name != 'Full_WDF'
    """
    return pd.read_sql(query, engine)

mapping_table = fetch_table_mappings(engine)

cnt = 1
# Function to process each table
def process_table(table_mapping, db_configs):
    global cnt
    start_time = time.time()
    server_name = table_mapping['server_name'].iloc[0]
    database_name = table_mapping['database_name'].iloc[0]
    schema_name = table_mapping['schema_name'].iloc[0]
    table_name = table_mapping['table_name'].iloc[0]
    
    print(f"{cnt}) Processing table: {table_name} from server: {server_name}")
    
    # Establish source connection using pooling
    source_config = db_configs[server_name]
    source_config['server'] = server_name
    source_engine = create_engine(create_connection_string(source_config, database_name))

    # Fetch data in batch
    column_names = table_mapping['column_name'].tolist()
    columns_str = ', '.join([f'"{col}"' for col in column_names])

    query = f"SELECT {columns_str} FROM {schema_name}.\"{table_name}\""
    source_data = pd.read_sql(query, source_engine)

    # Process and update in batches
    updates = []
    for col in column_names:
        grouped = source_data.groupby(col).size().reset_index(name='count')
        for _, row in grouped.iterrows():
            updates.append((row[col], json.dumps({"count": int(row['count'])}), table_name))

    # Perform batch updates
    update_query = """
        UPDATE da_new.test_company_master
        SET data_point = data_point || %s
        WHERE company_domain = %s
    """
    with engine.begin() as conn:
        conn.execute(update_query, updates)

    logger.info(f"Processed {table_name} in {time.time() - start_time} seconds")
    print(f"{cnt}) Finished processing table: {table_name}")
    cnt = cnt+1

# Process all tables
script_start_time = time.time()
logger.info("Script Started")

tables_grouped = mapping_table.groupby(['server_name', 'table_name'])
for (server_name, table_name), table_mapping in tables_grouped:
    process_table(table_mapping, db_configs)
    

logger.info(f"Script Completed in {time.time() - script_start_time} seconds")
