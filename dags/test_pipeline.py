import airflow
import json
from airflow import DAG
import logging
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import urllib.request
import urllib.parse
import pyodbc

server = 'demo-esg-reporting-server.database.windows.net'
database = 'demo-esg-reporting-sql-database'
username = 'esgadmin'
password = 'bmpi2022!'
driver= '{ODBC Driver 18 for SQL Server}'

openfigi_apikey = '06dc25b7-1e90-4748-b1ef-2d65d987c8cc'

azure = WasbHook(wasb_conn_id='blob_storage_conn')

args = {
    "owner": "BMPI",
    "start_date": datetime(2022,9,5)}

dag = DAG(
        dag_id="ESG_demo_pipeline",
        default_args=args,
        schedule_interval="7 8 * * *",
        dagrun_timeout=timedelta(minutes=1),
        tags=['poc', 'azure'])

def get_blob_list():
    logging.info("Request to storage is happening 1")
    return azure.get_blobs_list(container_name='airflow-input', prefix='order_')

def open_blobs():
        blob_list = get_blob_list()
        logging.info(blob_list)
        data_from_files = []
        for blob in blob_list: 
                my_file = azure.read_file(container_name='airflow-input', blob_name=blob)
                data_from_files.append(my_file)
        return data_from_files

def request_data_from_figi(isin):
        jobs = [{"idType":"ID_ISIN","idValue": isin}]
        handler = urllib.request.HTTPHandler()
        opener = urllib.request.build_opener(handler)
        openfigi_url = 'https://api.openfigi.com/v2/mapping'
        request = urllib.request.Request(openfigi_url, data=bytes(json.dumps(jobs), encoding='utf-8'))
        request.add_header('Content-Type','application/json')
        if openfigi_apikey:
                request.add_header('X-OPENFIGI-APIKEY', openfigi_apikey)
                request.get_method = lambda: 'POST'
                connection = opener.open(request)
                if connection.code != 200:
                        raise Exception('Bad response code {}'.format(str(response.status_code)))
                job_results = json.loads(connection.read().decode('utf-8'))

        for job, result in zip(jobs, job_results):
                figis = []
                figis.append([{"shareClassFIGI" : d['shareClassFIGI'], "name" : d['name']} for d in result.get('data', [])])
        return figis

def load_data_to_db():
        data = open_blobs()
        for order in data: 
                logging.info("Processing order:")
                logging.info(order)
                orderObj = json.loads(order)

                portfolioName = orderObj["portfolioName"]
                orderId = orderObj["orderId"]
                position = orderObj["positionHistory"][0]["positions"][0]

                figis = request_data_from_figi(position["isin"])
                figi = figis[0][0].get('shareClassFIGI','')
                name = figis[0][0].get('name','')
                logging.info("Writing the following figi value to dbo.Issuer")
                logging.info(figi)
                logging.info("Writing the following name value to dbo.Isuuer")
                logging.info(name)
                with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+password) as conn:
                        with conn.cursor() as cursor:
                                cursor.execute("INSERT INTO [dbo].[Portfolio] (name, orderId) VALUES ('" + portfolioName + "', " + orderId + ");")
                                cursor.execute("INSERT INTO [dbo].[Issuer] (figi, name) VALUES ('" + figi + "', '" + name + "');")

print_blob_list = PythonOperator(
        task_id='get_blob_list',
        python_callable=get_blob_list,
        dag=dag)

extract_data_from_blobs = PythonOperator(
        task_id='open_blobs', 
        python_callable=open_blobs,
        dag=dag
)   

load_data_to_db = PythonOperator(
        task_id = 'load_to_db',
        python_callable=load_data_to_db,
        dag=dag
)

print_blob_list >> extract_data_from_blobs >> load_data_to_db 
