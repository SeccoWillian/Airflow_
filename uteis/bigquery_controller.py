import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/...sample.json"
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryUpdateTableSchemaOperator,
    BigQueryUpdateTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "analytics-dev")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "datalake_files_dev")
GCP = "Google_Cloud_Import_Bigquery"
DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'DB')

class BigqueryController:
    def getLastLoad(tabela):
        try:
            client = bigquery.Client()
            query_job = client.query(
                (
                    "select max(bigquery_ingestion) as last_load "
                    "from `triider-analytics-dev.triiderDB.{}`"
                ).format(tabela)
            )
            results = query_job.result() 
            for row in results:
                return row.last_load
        except:
            print('Erro ao encontrar a tabela {}!'.format(tabela))
            return '2000-01-01'

    def criarDataset(dags):
        return BigQueryCreateEmptyDatasetOperator(
            task_id='create_bigquery_dataset', 
            gcp_conn_id=GCP,
            dataset_id=DATASET_NAME, 
            dag=dags
        )

    def carregaDados(TABLE_NAME, FILENAME, SCHEMA, TYPE, PARTITION, CLUSTER, dags):
        return GCSToBigQueryOperator(
            task_id='gcs_to_bigquery',
            bigquery_conn_id=GCP,
            bucket=GCS_BUCKET,
            source_objects=[FILENAME],
            source_format='NEWLINE_DELIMITED_JSON',
            encoding='UTF-8',
            destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
            schema_fields=SCHEMA,
            write_disposition=TYPE,
            time_partitioning=PARTITION,
            cluster_fields=CLUSTER,
            dag=dags,
        )
    
    def updateTableSchema(TABLE_NAME, DESCRIPTION, dags):
        return BigQueryUpdateTableSchemaOperator(
        task_id="update_table_schema",
        gcp_conn_id=GCP,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        schema_fields_updates=DESCRIPTION,
        dag=dags
    )

    def updateTableDescription(TABLE_NAME, FRIENDLY_NAME, DESCRIPTION_TABLE, dags):
        return BigQueryUpdateTableOperator(
        task_id="update_table_description",
        gcp_conn_id=GCP,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        table_resource={
            "friendlyName": FRIENDLY_NAME,
            "description": DESCRIPTION_TABLE,
        },
        dag=dags
    )