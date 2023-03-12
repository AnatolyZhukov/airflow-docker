from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import sqlite3
from datetime import date, timedelta
from Users/anatoliyzhukov/airflow-docker/scripts/scripts/Introduction_to_ETL_2_2 import extract_currency, \
    insert_to_db, extract_data

dag = DAG('dag',
           schedule_interval=timedelta(days=1),
           start_date=days_ago(1),
           retries=5,
           retry_delay=timedelta(minutes=5)
)

insert_exchange = PythonOperator(task_id='extract_currency',
                                 python_callable=insert_to_db,
                                 op_kwargs={'data': extract_currency('2021-01-01'),
                                            'table_name': 'currency',
                                            'conn': sqlite3.connect("./scripts/airflow_course.db")},
                                 dag=dag)

insert_data_git = PythonOperator(task_id='extract_currency',
                                 python_callable=insert_to_db,
                                 op_kwargs={'data': extract_data('2021-01-01'),
                                            'table_name': 'data',
                                            'conn': sqlite3.connect("./scripts/airflow_course.db")},
                                 dag=dag)

insert_exchange
insert_data_git
