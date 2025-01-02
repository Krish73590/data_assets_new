# import pandas as pd
# import json

# # Define data
# company_master = {
#     "holding.asp": {
#         "AWS_Startup_USA_Calling_List_Master": ["holding.asp", "holding.asp."],
#         "apollo_company_standard": ["holding.asp"]
#     }
# }

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
# print(unique_columns)
# # Initialize master output dictionary
# master_output = {}

# # Process data for each unique company_master column
# for column_master in unique_columns:
#     output = {}

#     # Process each source table
#     for source_name, data in source_data.items():
#         mapping = mapping_table[mapping_table['table_name'] == source_name]
#         domain_mapping = mapping[mapping['company_master'] == column_master]
#         print(domain_mapping)
#         if domain_mapping.empty:
#             continue

#         data_column = domain_mapping['column_name'].values[0]
#         print(data_column)
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
# print(json.dumps(master_output, indent=4))



import pandas as pd
import json

# Provided data and mappings
company_master = {
    "holding.asp": {
        "AWS_Startup_USA_Calling_List_Master": ["holding.asp", "holding.asp."],
        "apollo_company_standard": ["holding.asp"]
    }
}

mapping_table = pd.DataFrame({
    'table_name': [
        'AWS_Startup_USA_Calling_List_Master',
        'AWS_Startup_USA_Calling_List_Master',
        'AWS_Startup_USA_Calling_List_Master',
        'AWS_Startup_USA_Calling_List_Master',
        'AWS_Startup_USA_Calling_List_Master',
        'apollo_company_standard',
        'apollo_company_standard'
    ],
    'column_name': ['Website', 'Email', 'Email Source', 'Email Status', 'HQ Phone 1', 'HQ Phone', 'Website'],
    'company_master': ['company_domain', 'email', 'email_source_name', 'email_status', 'hq1_phone_number', 'hq1_phone_number', 'company_domain']
})

source_data = {
    'AWS_Startup_USA_Calling_List_Master': pd.DataFrame({
        'Website': ['holding.asp', 'holding.asp', 'holding.asp.'],
        'HQ Phone 1': ['123456789', '987654321', '123456789'],
        'Email': ['test@holding.asp.com', 'admin@holding.asp.com', ''],
        'Email Source': ['source1', 'source2', 'source3'],
        'Email Status': ['status1', 'status2', 'status3'],
        'ID': [1, 20, 1001]
    }),
    'apollo_company_standard': pd.DataFrame({
        'Website': ['holding.asp'],
        'HQ Phone': ['123456789'],
        'ID': [5]
    })
}

# Initialize master output dictionary by domains
master_output = {domain: {} for domain in company_master}

# Populate data by domain and associated tables
for domain, tables in company_master.items():
    for table, web_list in tables.items():
        data = source_data.get(table, pd.DataFrame())
        for web in web_list:
            if web not in master_output[domain]:
                master_output[domain][web] = {"details": [], "total_count": 0}
            # Fetch the relevant columns for each table from the mapping table
            relevant_columns = mapping_table[(mapping_table['table_name'] == table) & (mapping_table['company_master'] == 'company_domain')]['column_name'].tolist()
            for col in relevant_columns:
                for _, row in data.iterrows():
                    if row[col] == web:
                        source_detail = master_output[domain][web]
                        source_detail["details"].append({
                            "source_name": table,
                            "source_pk_id": row['ID'],
                            "data": {key: row[key] for key in data.columns if key != 'ID'}
                        })
                        source_detail["total_count"] += 1

# Output JSON
print(json.dumps(master_output, indent=4))
