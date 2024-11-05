from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
import extractData as extract
import transform as tr
from save_DB import save_data



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 23),  
    'email': ['santiago.gomez_cas@uao.edu.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'api_proyect_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',
) as dag:
    
    extract_API = PythonOperator(
        task_id='extract_API',
        python_callable=extract.load_API_data,
        provide_context=True,
    )
    
    extract_dataset = PythonOperator(
        task_id='extract_dataset',
        python_callable=extract.load_dataset,
        provide_context=True,
    )
    
    fact_table_created = PythonOperator(
        task_id='fact_table_created',
        python_callable=tr.process_data,
        provide_context=True,
    )
    
    save_info = PythonOperator(
        task_id='save_info',
        python_callable=save_data,
        provide_context=True,
    )
    
    clean_API = PythonOperator(
        task_id='clean_API',
        python_callable=tr.clean_api_data,
        provide_context=True,
    )
    ################################################
    # dimension_vehiculo_ = PythonOperator(
    #     task_id='dimension_vehiculo',
    #     python_callable=tr.dimensions_vehiculo,
    #     provide_context=True,
    # )
        
    # dimension_vendedor_ = PythonOperator(
    #     task_id='dimension_vendedor',
    #     python_callable=tr.dimension_vendedor,
    #     provide_context=True,
    # )
    
    # dimension_ratings_ = PythonOperator(
    #     task_id='dimension_ratings',
    #     python_callable=tr.dimension_ratings,
    #     provide_context=True,
    # )
    
    
extract_API 
extract_dataset >> clean_API >> fact_table_created >> save_info