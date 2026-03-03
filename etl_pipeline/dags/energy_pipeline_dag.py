from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 2. Imports das funções originais
from scr.extract import extract_data
from scr.transform.text import normalize_text_columns, normalize_text_data, clean_text_data
from scr.transform.numeric import clean_numeric_data, fill_nan_numeric_data, round_metrics
from scr.load import load_data

# 3. Função Wrapper (Ponte)
def run_full_transformation_and_load():
    print("Iniciando Extração...")
    df = extract_data()
    
    print("Iniciando Transformação...")
    df = normalize_text_columns(df)
    df = clean_text_data(df)
    df = clean_numeric_data(df)
    df = round_metrics(df)
    
    print("Iniciando Carga no DW...")
    load_data(df)
    print("Processo concluído com sucesso!")

# 4. Configurações padrão
default_args = {
    'owner': 'Luis Fernando',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 5. Definição da DAG
with DAG(
    dag_id='renewable_energy_etl_v1',
    default_args=default_args,
    description='Pipeline de Energia Renovável - Extração até Carga',
    schedule_interval='@daily',
    catchup=False,
    tags=['energy', 'postgres'],
) as dag:

    # 6. Tarefa única que orquestra suas funções
    etl_task = PythonOperator(
        task_id='run_complete_etl',
        python_callable=run_full_transformation_and_load
    )

    etl_task