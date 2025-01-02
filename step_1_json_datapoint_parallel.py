import json
import time
import psycopg2
from psycopg2 import sql
import logging
import pandas as pd
from sqlalchemy import create_engine
import urllib
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.custom_logger import CustomLogger

logger_class = CustomLogger('data_assets_count', level=logging.INFO)
logger = logger_class.setup_logger('data_asset')


# Load configuration
def load_config():
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

encoded_password = urllib.parse.quote_plus(PASSWORD)
connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{encoded_password}@{ENDPOINT}:{PORT}/{DATABASE}"
engine = create_engine(connection_string)

# Load mapping data
query = "SELECT company_domain, source_table from da_new.test_stg_company_mix"
mapping_df = pd.read_sql(query, engine)

def safe_eval(x):
    if isinstance(x, str):
        try:
            return eval(x)
        except Exception as e:
            print(f"Error evaluating: {x} - {e}")
            return None
    return x

mapping_df['source_table'] = mapping_df['source_table'].apply(safe_eval)

query = """
    SELECT a.server_name, a.database_name, a.schema_name, a.table_name, a.column_name, test_stg_company_mix
    FROM da_new.test_tbl_source_mapping_master a 
    WHERE test_stg_company_mix is not null AND a.table_name <> 'Full_WDF'  -- ='AB_Lead Enrichment_Project_Final Master';
"""
mapping_table = pd.read_sql(query, engine)

# ----------------------------------------
# Parallel processing function for tables
# ----------------------------------------

def process_table(table_name, cnt):
    try:
        # Database connections for parallel execution
        source_connection = None
        source_cursor = None
        target_connection = engine.raw_connection()
        target_cursor = target_connection.cursor()

        start_time = time.time()
        logger.info(f"{cnt}) Started Processing table: {table_name}")

        # Query source domain details
        query = f"""
            SELECT a.source_table, domain_json, domain_text, column_name
            FROM da_new.test_source_table_223 a 
            INNER JOIN da_new.test_stg_company_mix b 
            ON a.domain_text = b.company_domain WHERE a.source_table = '{table_name}';
        """
        source_domain_details = pd.read_sql(query, engine)
        table_mapping = mapping_table[mapping_table['table_name'] == table_name]

        # Extract server and schema info
        server_name = table_mapping['server_name'].iloc[0]
        database_name = table_mapping['database_name'].iloc[0]
        schema_name = table_mapping['schema_name'].iloc[0]
        
        # Get server config
        if server_name not in db_configs:
            logger.warning(f"Server '{server_name}' not found in the configuration file.")
            return

        source_config = db_configs[server_name]

        # Connect to source database
        source_connection = psycopg2.connect(
            host=server_name,
            port=source_config['port'],
            user=source_config['user'],
            password=source_config['password'],
            database=database_name
        )
        source_cursor = source_connection.cursor()

        # Get required columns
        column_required = table_mapping['column_name'].to_list()
        column_str = ', '.join([f'"{col}"' for col in column_required])
        query = f""" SELECT {column_str} FROM {schema_name}."{table_name}"; """
        source_cursor.execute(query)
        data_to_insert = source_cursor.fetchall()
        data = pd.DataFrame(data_to_insert, columns=column_required)
        unique_columns = ['company_revenue_range_2',
'email',
'company_primary_industry_source_name',
'company_source_name',
'instagram_company_url',
'email_ia_notes',
'company_revenue_2_source_link',
'company_revenue_1',
'hq4_phone_source',
'hq4_phone_number',
'address_source_url',
'company_secondary_industry',
'company_phone',
'twitter_company_url_handle',
'company_city',
'phone_status_voice',
'company_full_address',
'phone_source_name',
'facebook_company_url',
'company_revenue_2',
'company_employees_1',
'hq4_phone_source_link',
'email_source_name',
't_source_company_id',
'company_employees_1_source_name',
'company_primary_industry',
'company_employees_range_2',
'email_service_provider',
'company_country',
'hq5_phone_number',
'company_zip_postcode',
'phone_type',
'company_employees_2_source_link',
'company_secondary_industry_source_link',
'company_cin_number',
'company_sic_code_1',
'address_source',
'linkedin_company_url',
'hq5_phone_source',
'latitude',
'hq3_phone_source',
'company_revenue_1_source_name',
'additional_address_line_1',
'company_name',
'company_revenue_1_source_link',
'hq1_phone_source_link',
'phone_number',
'company_primary_industry_source_link',
'hq3_phone_number',
'hq2_phone_number',
'phone_source_link',
'company_state_province',
'company_website',
'hq2_phone_source_link',
'is_domain_active',
'hq1_phone_number',
'naics_name',
'email_strikeiron_status',
'company_source_link',
'company_revenue_range_1',
'email_source_link',
'company_revenue_range_1_source_link',
'date_and_time_created',
'hq5_phone_source_link',
'company_industry',
'company_employees_range_1',
'additional_address_line_2',
't_source_id',
'company_naics_code_2',
'hq3_phone_source_link',
'hq1_phone_source',
'company_employees_range_1_source_link',
'youtube_company_url',
'company_sic_code_2',
'email_status',
'rpf_filename',
'company_revenue_range_1_source_name',
'company_employees_1_source_link',
'company_employees_2',
'hq2_phone_source',
'crunchbase_url',
'company_naics_code_1',
'email_zb_status',
'date_and_time_last_updated',
'longitude',
'company_employees_range_1_source_name'
]
        pk_column = 't_source_id'
        pk_mapping = table_mapping[table_mapping['test_stg_company_mix'] == pk_column]
        if pk_mapping.empty:
            raise ValueError(f"Primary key '{pk_column}' not found in mapping table.")
        pk_column = pk_mapping['column_name'].values[0]
        
        # Process each data point
        for data_point in unique_columns:
            source_data_point_map = table_mapping[table_mapping['test_stg_company_mix'] == data_point]
            for index, row in source_domain_details.iterrows():
                domain_text = row['domain_text']
                domain_json = row['domain_json']
                domain_column_name = row['column_name']

                output = {}
                if source_data_point_map.empty:
                    continue

                source_data_point_col_name = source_data_point_map['column_name'].values[0]
                filtered_df = data[data[domain_column_name].isin(domain_json)]

                for _, row in filtered_df.iterrows():
                    key_value = row[source_data_point_col_name]
                    fetch_query = f"SELECT {data_point} FROM da_new.test_stg_company_mix WHERE company_domain = '{domain_text}'"
                    target_cursor.execute(fetch_query)
                    existing_json = target_cursor.fetchone()
                    
                    if existing_json and existing_json[0]:
                        output = existing_json[0]
                        # output = json.loads(existing_json[0])
                    else:
                        output = {}
                    if key_value not in output:
                        output[key_value] = {"details": [], "total_count": 0, "source_count": 0}

                    source_detail = output[key_value]
                    source_index = next((i for i, detail in enumerate(source_detail['details'])
                                         if table_name in detail["source_name"]), None)

                    if source_index is None:
                        source_detail['details'].append({
                            "source_name": [table_name],
                            "source_pk_id": [row[pk_column]],
                            "count": 1
                        })
                        source_detail['source_count'] += 1
                    else:
                        current_detail = source_detail['details'][source_index]
                        if row[pk_column] not in current_detail['source_pk_id']:
                            current_detail['source_pk_id'].append(row[pk_column])
                            current_detail['count'] += 1

                    source_detail['total_count'] = sum(detail['count'] for detail in source_detail['details'])
                
                if {json.dumps(output, indent=4)} != {}:
                    update_query = sql.SQL("UPDATE da_new.test_stg_company_mix SET {} = %s WHERE company_domain = %s").format(sql.Identifier(data_point))
                    # update_query = """UPDATE da_new.test_stg_company_mix
                    #           SET {} = %s
                    #           WHERE company_domain = %s""".format(sql.Identifier(data_point))
                    target_cursor.execute(update_query, (json.dumps(output), domain_text))
                    
                    target_connection.commit()
                    # print('reached')
                    
                    # query = f"""UPDATE da_new.test_stg_company_mix
                    #             SET {data_point} = '{json.dumps(output, indent=4)}'
                    #             WHERE company_domain = '{domain_text}'"""
                    # target_cursor.execute(query)
                    # target_connection.commit()
                else:
                    print(f'Error in source column mapping: {table_name} in {source_data_point_col_name}')
            # print(data_point)
        end_time = time.time()
        logger.info(f"{cnt}) Completed Processing table: {table_name} in {end_time - start_time} seconds")

    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
    finally:
        if source_cursor:
            source_cursor.close()
        if source_connection:
            source_connection.close()
        if target_cursor:
            target_cursor.close()
        if target_connection:
            target_connection.close()


# -----------------------
# Execute Parallel Tasks
# -----------------------
unique_table_names = mapping_table['table_name'].unique()
script_start_time = time.time()

with ThreadPoolExecutor(max_workers=12) as executor:  # Adjust workers based on CPU
    future_to_table = {executor.submit(process_table, table_name, cnt): table_name for cnt, table_name in enumerate(unique_table_names, 1)}

    for future in as_completed(future_to_table):
        table_name = future_to_table[future]
        try:
            future.result()
        except Exception as e:
            logger.error(f"Exception occurred for table {table_name}: {e}")

logger.info(f"Total Execution Time: {time.time() - script_start_time} seconds")
