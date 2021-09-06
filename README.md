# Airflow

## Para criação de um ambiente SandBox (local)

Seguir os passos para a instalação do Airflow neste [site](https://airflow.apache.org/) 

Executar primeiro o webserver em um console e em outro console executar o scheduler

> airflow webserver

> airflow scheduler


## Para ambiente de produção

 
Utilizar o docker-compose.yaml para subir os containers

- Instalar Docker

- Instalar docker-compose

- no local onde está o documento docker-compose.yml 
	- > docker-compose up airflow-init 

  
### - DAGS

Todas as dags ficam dentro da pasta dags e para alguns jobs específicos, as classes ficam disponíveis na pasta uteis

**Uteis/postgres_query.py**

Aqui se encontra todas as SQLs para extração de dados

**Uteis/bigquery_schema.py**

Aqui se encontra o schema de cada um dos jobs e os parâmetros de importação para o BigQuery
