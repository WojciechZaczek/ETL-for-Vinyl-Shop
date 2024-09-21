from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from functionality.extract import Extract
from functionality.transform import transform_csv_files
from functionality.connections import create_conn_airflow
# from functionality.load_pandas import Load
import os


# def extract_data():
#     source_folder = r'C:\Users\wojci\PycharmProjects\Vinyl_shop\source'
#     archive_folder = os.path.join(source_folder, 'archive')
#     inbox_folder = 'inbox'
#     extractor = Extract(inbox_folder, source_folder, archive_folder)
#     extractor.extract_files_to_inbox()

#
# def transform_data():
#     input_folder = 'inbox'
#     output_folder = 'data'
#     archive_folder = os.path.join('inbox', 'archive')
#     transform_csv_files(input_folder, output_folder, archive_folder)


# def load_data():
#     transformed_file = os.path.join('transformed', 'vinyl_database_transformed.csv')
#     archive_folder = os.path.join('archive')
#     loader = Load(transformed_file, archive_folder)
#     loader.load_data()


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 10),
    'retries': 1,
}


dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
)

create_conn_task = PythonOperator(
    task_id='create_connection',
    python_callable=create_conn_airflow,
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=Extract().extract_files_to_inbox(),
    dag=dag
)

# transform_task = PythonOperator(
#     task_id='transform',
#     python_callable=transform_data,
#     dag=dag
# )

# load_task = PythonOperator(
#     task_id='load',
#     python_callable=load_data,
#     dag=dag
# )

create_conn_task >> extract_task #>> transform_task #>> load_task
