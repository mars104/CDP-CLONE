from google.cloud import bigquery
# the following script will truncate all tables in our clone

# Set up the BigQuery client
client = bigquery.Client()

# Set the target database
target_db = "dmn01-rsksoi-bld-01-2017.dmn01_rsksoi_euwe2_rsk_csp_cus_view"

# Get the list of tables in the target database
target_tables_query = f"SELECT table_name FROM `{target_db}.INFORMATION_SCHEMA.TABLES`"
target_tables = [row['table_name'] for row in client.query(target_tables_query).result()]

# Truncate all tables in the target database
for target_table in target_tables:
    truncate_query = f"TRUNCATE TABLE `{target_db}.{target_table}`"
    client.query(truncate_query).result()
    print(f"Truncated table {target_table}")
