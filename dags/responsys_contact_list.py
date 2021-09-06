import sys
import paramiko
import pandas as pd

sys.path.insert(0, '/')

from datetime import datetime, timedelta
from postgres_query import PostgresQuery
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date':datetime.now(),
    'retries':1,
    'retry_delay':timedelta(seconds=20)
}

def get_activated_sources():
    request = PostgresQuery().responsys_contact_list
    pg_hook = PostgresHook(postgres_conn_id='Postgres_Triider_Consulta', schema='triider_owner')
    connection = pg_hook.get_conn()
    sql_query = pd.read_sql_query(request, connection) 

    df = pd.DataFrame(sql_query)
    df.to_csv (r'/contact_list.csv', index = False, encoding="utf-8") 

def salvaSFTPResponsys():
    paramiko.util.log_to_file("paramiko.log")

    filepath = "/contact_list.csv"
    localpath = "/contact_list.csv"

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    key = paramiko.RSAKey.from_private_key_file('privateKey.pem', password=Variable.get("ResponsysPass"))
    ssh.connect(Variable.get("ResponsysHost"), username=Variable.get("ResponsysUser"), pkey=key)

    sftp = ssh.open_sftp()
    sftp.put(localpath, filepath)
    ssh.close()
    

with DAG('Responsys_contact_list',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False) as dag:

    start_task = DummyOperator(task_id='start_task')
    hook_task = PythonOperator(task_id='postgres_to_local', python_callable=get_activated_sources)
    transfer_sftp_task = PythonOperator(task_id='local_to_sftp', python_callable=salvaSFTPResponsys)
   

    start_task >> hook_task >> transfer_sftp_task