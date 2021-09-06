# DAG service

import os
import sys

sys.path.insert(0, '/airflow/uteis')

from airflow import models
from airflow.utils.dates import days_ago, datetime
from postgres_query import PostgresQuery
from bigquery_schema import BigquerySchema
from postgres_controller import PostgresController
from bigquery_controller import BigqueryController

#-------------------- INFORME OS DADOS ABAIXO -------------------------------------------
POSTGRES_TABLE = 'service'                          #definir a tabela de origem do postgres
BQ_DICTIONARY = BigquerySchema().service
QUERY = PostgresQuery().service                       
#----------------------------------------------------------------------------------------

FRIENDLY_NAME = BQ_DICTIONARY[1]               
DESCRIPTION_TABLE = BQ_DICTIONARY[2]           
SCHEMA = BQ_DICTIONARY[3]                      
DESCRIPTION = BQ_DICTIONARY[4]                 
TYPE = BQ_DICTIONARY[5]
TAG = BQ_DICTIONARY[6]   
SCHEDULER = BQ_DICTIONARY[7]     
PARTITION = BQ_DICTIONARY[8]    
CLUSTER = BQ_DICTIONARY[9]                   

MAIN = POSTGRES_TABLE.lower()
LAST_LOAD = BigqueryController.getLastLoad(MAIN) 
FILENAME = MAIN +'.json'
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", MAIN)
if(TYPE == 'WRITE_TRUNCATE'):
    SQL_QUERY = QUERY
else:
    SQL_QUERY = QUERY.format(LAST_LOAD, LAST_LOAD)

with models.DAG(
    dag_id='triider_'+ MAIN,
    schedule_interval=SCHEDULER,  
    start_date=datetime.now(),
    tags=TAG,
) as dag:
    postgresSQL = PostgresController.consulta(SQL_QUERY, FILENAME)
    gcpDataset = BigqueryController.criarDataset(dag)
    gcpBigquery = BigqueryController.carregaDados(TABLE_NAME, FILENAME, SCHEMA, TYPE, PARTITION, CLUSTER, dag)
    updateBigquerySchema = BigqueryController.updateTableSchema(TABLE_NAME, DESCRIPTION, dag)
    updateBigqueryDescription = BigqueryController.updateTableDescription(TABLE_NAME, FRIENDLY_NAME, DESCRIPTION_TABLE, dag)

    postgresSQL >> gcpDataset >> gcpBigquery >> [updateBigquerySchema, updateBigqueryDescription]