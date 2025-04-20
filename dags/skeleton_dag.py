from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from acme.operators.load_data_opeartor import LoadDataOperator
from acme.operators.preprocessing_operator import PreprocessingOperator
from acme.operators.chunking_operator import ChunkingOperator
from acme.operators.embedding_operator import EmbeddingOperator
# from acme.operators.save_into_opeansearch import SaveIntoOpenSearchOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='skeleton_data_pipeline',
    default_args=default_args,
    description='A skeleton DAG for data processing pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['skeleton', 'data-pipeline']
) as dag:

    # Task 1: Load Data
    load_data = LoadDataOperator(
        task_id='load_data',
        data_source='/opt/airflow/dags/data',  # Replace with actual data source
        # connection_params={
        #     'dialect': 'postgresql',
        #     'username': 'your_username',
        #     'password': 'your_password',
        #     'host': 'your_host',
        #     'database': 'your_database'
        # }
    )

    # Task 2: Preprocess Data
    preprocess_data = PreprocessingOperator(
        task_id='preprocess_data',
        input_data="load_data",
        input_type='office_document',  # or 'image', 'office_document' based on your data
        preprocessing_params={
            'normalize': True,
            'remove_outliers': True
        }
    )

    # Task 3: Chunk Data
    chunk_data = ChunkingOperator(
        task_id='chunk_data',
        input_data="preprocess_data",
        input_type='text',  # or 'image', 'rdb' based on your data
        strategy='semantic',  # or 'fixed', 'recursive', 'document', 'agentic'
        strategy_params={
            'chunk_size': 1000,
            'chunk_overlap': 200
        }
    )

    # Task 4: Generate Embeddings
    generate_embeddings = EmbeddingOperator(
        task_id='generate_embeddings',
        input_data="chunk_data",
        input_type='text',  # or 'image' based on your data
        model_name='sentence-transformers/all-MiniLM-L6-v2',  # or other model
        batch_size=32,
        normalize=True
    )

    # # Task 5: Save to OpenSearch
    # save_to_opensearch = SaveIntoOpenSearchOperator(
    #     task_id='save_to_opensearch',
    #     embeddings="{{ task_instance.xcom_pull(task_ids='generate_embeddings') }}",
    #     documents="{{ task_instance.xcom_pull(task_ids='chunk_data') }}",
    #     index_name='your_index_name',
    #     opensearch_conn={
    #         'hosts': ['https://your-opensearch-host:9200'],
    #         'http_auth': ('username', 'password'),
    #         'use_ssl': True,
    #         'verify_certs': True
    #     },
    #     batch_size=500
    # )

    # Define task dependencies
    load_data >> preprocess_data >> chunk_data >> generate_embeddings 
    # pre_embedding_process >> embedding_process
    # embedding_process >> post_embedding_process
    # post_embedding_process >> save_into_opensearch