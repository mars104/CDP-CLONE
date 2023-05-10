from google.cloud import bigquery

#the following script will clone customers from CDP to our clone based on customers.csv file with a list of customer id's from which to clone
import csv
from google.cloud import bigquery

# Set up the BigQuery client
client = bigquery.Client()

# Set the source and target databases
source_db = "dmn01-cussoi-bld-01-ccd8.dmn01_cus_ccs_view"
target_db = "dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_cus_view"

# Get the list of tables in the source and target databases
source_tables_query = f"SELECT table_name FROM `{source_db}.INFORMATION_SCHEMA.TABLES`"
source_tables = [row['table_name'] for row in client.query(source_tables_query).result()]

target_tables_query = f"SELECT table_name FROM `{target_db}.INFORMATION_SCHEMA.TABLES`"
target_tables = [row['table_name'] for row in client.query(target_tables_query).result()]

# Function to cast specific columns in the source table
def cast_source_column(column_name):
    if column_name == 'phone_number' or column_name == 'phone_ext_number':
        return f"SAFE_CAST({column_name} AS STRING)"
    else:
        return column_name

# Read customer IDs from the customers.csv file
customer_ids = []
with open('customers.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        customer_ids.append(row[0])

customer_ids_str = ', '.join([f"'{customer_id}'" for customer_id in customer_ids])

# Clone the specified customers from every table in the source to every table in the target
for source_table, target_table in zip(source_tables, target_tables):
    # Get the list of common columns between source and target tables
    common_columns_query = f"""
    SELECT source_columns.column_name
    FROM `{source_db}.INFORMATION_SCHEMA.COLUMNS` source_columns
    JOIN `{target_db}.INFORMATION_SCHEMA.COLUMNS` target_columns
    ON source_columns.column_name = target_columns.column_name
    WHERE source_columns.table_name = '{source_table}' AND target_columns.table_name = '{target_table}'
    """
    common_columns = [row['column_name'] for row in client.query(common_columns_query).result()]
    common_columns_str = ', '.join(common_columns)
    common_columns_casted_str = ', '.join([cast_source_column(col) for col in common_columns])

    clone_query = f"""
    INSERT INTO `{target_db}.{target_table}` ({common_columns_str})
    SELECT {common_columns_casted_str}
    FROM `{source_db}.{source_table}`
    WHERE customer_id IN ({customer_ids_str})
    """
    client.query(clone_query).result()
    print(f"Copied specified customers from {source_table} to {target_table}")
