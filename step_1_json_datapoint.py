import json
import time
import psycopg2
from psycopg2 import sql
import logging
import pandas as pd
from sqlalchemy import create_engine
import urllib
from utils.custom_logger import CustomLogger
# from sqlalchemy import create_engine

logger_class = CustomLogger('data_assets_count',level=logging.INFO)

logger = logger_class.setup_logger('data_asset')

def load_config():
    # Load the database configuration file
    with open('db_config.json', 'r') as f:
        return json.load(f)


db_configs = load_config()  
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
ENDPOINT = '192.168.3.243'
USER = db_configs[ENDPOINT]['user']
PASSWORD = db_configs[ENDPOINT]['password']
PORT = db_configs[ENDPOINT]['port']
DATABASE = 'data_assets'


# Encode the password for URL-safe transmission
encoded_password = urllib.parse.quote_plus(PASSWORD)

# Create connection string and engine for the target database
connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{encoded_password}@{ENDPOINT}:{PORT}/{DATABASE}"
engine = create_engine(connection_string)


query = "SELECT company_domain,source_table from da_new.test_company_master"
mapping_df = pd.read_sql(query, engine)

# Ensure all entries in 'source_table' are evaluated safely
def safe_eval(x):
    if isinstance(x, str):
        try:
            return eval(x)
        except Exception as e:
            print(f"Error evaluating: {x} - {e}")
            return None
    return x

mapping_df['source_table'] = mapping_df['source_table'].apply(safe_eval)

# Initialize an empty dictionary to store the data in the desired format
company_master = {}

# Populate the dictionary
for index, row in mapping_df.iterrows():
    company_master[row['company_domain']] = row['source_table']

# print(company_master)

with open('company_master.json', 'w') as f:
    json.dump(company_master, f, indent=4)
    
    
# Define data
# company_master = {
#     "holding.asp": {
#         "AWS_Startup_USA_Calling_List_Master": ["holding.asp", "holding.asp."],
#         "apollo_company_standard": ["holding.asp"]
#     }
# }

query = """
    SELECT a.server_name, a.database_name, a.schema_name, a.table_name, a.column_name, company_master
    FROM da_new.test_tbl_source_mapping_master a where company_master is not null
;
"""

# query = """
#     SELECT a.server_name, a.database_name, a.schema_name, a.table_name, a.column_name, company_master
#     FROM da_new.test_tbl_source_mapping_master a where a.table_name in (
# 'AB Seller Japan_Delivery Master_Krunal_072621','Amazon_japan_seller') and company_master is not null
# ;
# """

# Fetch the mappings from the database (target - localhost)
mapping_table = pd.read_sql(query, engine)

# Open a raw connection to the target database (localhost)
target_connection = engine.raw_connection()
source_connection = None
source_cursor = None
cnt = 0



try:
    # Create a cursor object for the target database
    target_cursor = target_connection.cursor()
    script_start_time = time.time()
    # print(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
    logger.info(f"Started!!!")
    # Iterate over each unique table_name
    unique_table_names = mapping_table['table_name'].unique()

    for table_name in unique_table_names:
        cnt = cnt+1
        start_time = time.time()
        # print(f"{cnt}) Processing table: {table_name} - Start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
        logger.info(f"{cnt}) Started Processing table: {table_name} ")
        try:
            query = f"""
                    SELECT a.source_table,domain_json,domain_text,column_name
                    FROM da_new.test_source_table_223 a inner join da_new.test_company_master b on a.domain_text = b.company_domain WHERE a.source_table = '{table_name}'
                ;
                """
            source_wise_domain_df = pd.read_sql(query, engine)
            # Filter the DataFrame for the current table_name
            table_mapping = mapping_table[mapping_table['table_name'] == table_name]
            source_domain_details = source_wise_domain_df[source_wise_domain_df['source_table'] == table_name]
            # Get the server_name, database_name, and schema_name for the current table_name
            server_name = table_mapping['server_name'].iloc[0]
            database_name = table_mapping['database_name'].iloc[0]
            schema_name = table_mapping['schema_name'].iloc[0]
            
            # Get the source database configuration based on the server_name
            if server_name not in db_configs:
                # print(f"Server '{server_name}' not found in the configuration file.")
                logger.warning(f"Server '{server_name}' not found in the configuration file.")
                continue  # Skip if server configuration is not found
            
            source_config = db_configs[server_name]
            
            # Create a new connection to the source database
            try:
                source_connection = psycopg2.connect(
                    host=server_name,
                    port=source_config['port'],
                    user=source_config['user'],
                    password=source_config['password'],
                    database=database_name  # Use the database name from the table
                )
                source_cursor = source_connection.cursor()
                
                column_required = table_mapping['column_name'].to_list()
                column_str = [ f""" "{col}" """ for col in column_required]
                column_str = ', '.join(column_str)

                
                query = f""" SELECT {column_str} from {schema_name}."{table_name}"; """
                # result = target_cursor.execute(query)
                source_cursor.execute(query)
                data_to_insert = source_cursor.fetchall()
                data = pd.DataFrame(data_to_insert, columns=column_required)

                unique_columns = ['company_zip_postcode']
                master_output = {}
                pk_column = 't_source_id'
                pk_mapping = table_mapping[table_mapping['company_master'] == pk_column]
                pk_column = pk_mapping['column_name'].values[0]
                for data_point in unique_columns:
                    source_data_point_map = table_mapping[table_mapping['company_master'] == data_point]

                    for index, row in source_domain_details.iterrows():
                        domain_text = row['domain_text']

                        domain_json = row['domain_json']  # Assuming this is a JSON string
                        domain_column_name = row['column_name']
                      
                        output = {}
                        
                        if source_data_point_map.empty:
                            continue

                        source_data_point_col_name = source_data_point_map['column_name'].values[0]

                        filtered_df = data[data[domain_column_name].isin(domain_json)]

                        for _, row in filtered_df.iterrows():
                            key_value = row[source_data_point_col_name]
                            # Fetch existing JSON from the database
                            fetch_query = f"SELECT {data_point} FROM da_new.test_company_master WHERE company_domain = '{domain_text}'"
                            target_cursor.execute(fetch_query)
                            existing_json = target_cursor.fetchone()

                            
                            if existing_json and existing_json[0]:
                                output = existing_json[0]
                            else:
                                output = {}
                            if key_value not in output:
                                output[key_value] = {"details": [], "total_count": 0, "source_count": 0}

                            source_detail = output[key_value]
                            # Find the existing source detail if it exists
                            source_index = next((index for index, detail in enumerate(source_detail['details'])
                                                if table_name in detail["source_name"]), None)

                            if source_index is None:
                                # If not found, create a new entry for this source
                                source_detail['details'].append({
                                    "source_name": [table_name],
                                    "source_pk_id": [row[pk_column]],
                                    "count": 1
                                })
                                source_detail['source_count'] += 1
                            else:
                                # If found, update the existing entry
                                current_detail = source_detail['details'][source_index]
                                if row[pk_column] not in current_detail['source_pk_id']:
                                    current_detail['source_pk_id'].append(row[pk_column])
                                    current_detail['count'] += 1  # Increase count only if new PK is added

                            # Always increment the total count
                            source_detail['total_count'] = sum(detail['count'] for detail in source_detail['details'])
                        
                        query = f""" UPDATE  da_new.test_company_master
                                    SET {data_point} = '{json.dumps(output, indent=4)}'
                                    WHERE company_domain = '{domain_text}' 
                                    """
                        target_cursor.execute(query)
                        target_connection.commit()
                        # master_output[data_point] = output
                
                end_time = time.time()
                logger.info(f"{cnt}) Completed Processing table: {table_name} - End time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
                logger.info(f"Time taken for table {table_name}: {end_time - start_time} seconds")    
                # print(f"{cnt}) Processing table: {table_name} - End time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
            except psycopg2.Error as e:
                print(f"Error connecting to source database on server '{server_name}': {e}")
                logger.error(f"Error connecting to source database on server '{server_name}': {e}")
                continue  # Skip if unable to connect to the source database
            finally:
                if source_cursor:
                    source_cursor.close()
                if source_connection:
                    source_connection.close()
        except Exception as e:
                # Log error for outer exceptions
                remarks = f"Error processing table {table_name}: {e}"
                logger.error(remarks)
finally:
    # Close the cursor and connections conditionally
    if target_cursor:
        target_cursor.close()
    if target_connection:
        target_connection.close()

script_end_time = time.time()
# print(f"{cnt}) Processing table: {table_name} - End time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
logger.info(f"Total Time taken : {script_end_time - script_start_time} seconds")
# print(f"Time taken for table {table_name}: {end_time - start_time} seconds")
     
        
# mapping_table = pd.DataFrame({
#     'table_name': [
#         'AWS_Startup_USA_Calling_List_Master',
#         'AWS_Startup_USA_Calling_List_Master',
#         'AWS_Startup_USA_Calling_List_Master',
#         'AWS_Startup_USA_Calling_List_Master',
#         'AWS_Startup_USA_Calling_List_Master',
#         'apollo_company_standard',
#         'apollo_company_standard'
#     ],
#     'column_name': ['Website', 'Email', 'Email Source', 'Email Status', 'HQ Phone 1', 'HQ Phone','Website'],
#     'company_master': ['company_domain', 'email', 'email_source_name', 'email_status', 'hq1_phone_number', 'hq1_phone_number','company_domain']
# })

# source_data = {
#     'AWS_Startup_USA_Calling_List_Master': pd.DataFrame({
#         'Website': ['holding.asp', 'holding.asp', 'holding.asp.'],
#         'HQ Phone 1': ['123456789', '987654321', '123456789'],
#         'Email': ['test@holding.asp.com', 'admin@holding.asp.com', ''],
#         'Email Source': ['source1', 'source2', 'source3'],
#         'Email Status': ['status1', 'status2', 'status3'],
#         'ID': [1, 20, 1001]
#     }),
#     'apollo_company_standard': pd.DataFrame({
#         'Website': ['holding.asp'],
#         'HQ Phone': ['123456789'],
#         'ID': [5]
#     })
# }

# # Get unique company_master columns to process each
# unique_columns = mapping_table['company_master'].unique()
# # print(unique_columns)
# # Initialize master output dictionary
# master_output = {}

# # Process data for each unique company_master column
# for column_master in unique_columns:
#     output = {}

#     # Process each source table
#     for source_name, data in source_data.items():
#         mapping = mapping_table[mapping_table['table_name'] == source_name]
#         domain_mapping = mapping[mapping['company_master'] == column_master]
        
#         if domain_mapping.empty:
#             continue

#         data_column = domain_mapping['column_name'].values[0]

#         for _, row in data.iterrows():
#             key_value = row[data_column]
#             if key_value not in output:
#                 output[key_value] = {"details": [], "total_count": 0, "source_count": 0}
            
#             source_detail = output[key_value]
#             source_index = next((index for index, detail in enumerate(source_detail['details']) if detail["source_name"] == source_name), None)
            
#             if source_index is None:
#                 source_detail['details'].append({
#                     "source_name": source_name,
#                     "source_pk_id": [row['ID']],
#                     "count": 1
#                 })
#                 source_detail['source_count'] += 1
#             else:
#                 current_detail = source_detail['details'][source_index]
#                 current_detail['source_pk_id'].append(row['ID'])
#                 current_detail['count'] += 1
            
#             source_detail['total_count'] += 1

#     # Assign the output for the specific company_master column
#     master_output[column_master] = output

# # Output JSON
# # print(json.dumps(master_output, indent=4))
