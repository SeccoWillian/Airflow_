import os

from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "datalake_files_dev")
CONNECTION = "Postgres_Triider_Consulta"
GCP = "Google_Cloud_Import_Bigquery"

class PostgresController:

    def consulta(SQL_QUERY, FILENAME):
        return PostgresToGCSOperator(
            task_id="postgress_to_gcs", 
            postgres_conn_id=CONNECTION,
            gcp_conn_id=GCP,
            sql=SQL_QUERY, 
            bucket=GCS_BUCKET, 
            filename=FILENAME, 
            gzip=False
        )