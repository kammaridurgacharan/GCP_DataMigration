from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage

service_account_path = "Local_path"

project_id = "Name_of_the_Project"

bucket_name = "Name_of_bucket"
file_names = ["File_Name.csv", "File_Name.csv", "File_Name3.csv"]

credentials = service_account.Credentials.from_service_account_file(service_account_path)

bq_client = bigquery.Client(project=project_id, credentials=credentials)

dataset_id = "Name_of_datast"
dataset_ref = bq_client.dataset(dataset_id)
try:
    bq_client.get_dataset(dataset_ref)
    print(f"Dataset {dataset_id} already exists.")
except NotFound:
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "Location"
    dataset = bq_client.create_dataset(dataset)
    print(f"Dataset {dataset_id} created.")

for file_name in file_names:
    table_id = file_name.split(".")[0]
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )
    uri = f"gs://{bucket_name}/World/{file_name}"
    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    print(f"Loading {file_name} to BigQuery...")
    load_job.result()
    print(f"Loaded {file_name} to BigQuery table {table_id}.")

print("\nHello, All CSV files loaded '{}' dataset in BigQuery.\n".format(dataset_id))