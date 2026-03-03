from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
import os

# Força a raiz do Airflow no Docker
sys.path.insert(0, os.path.abspath("/opt/airflow"))

try:
    # Importações seguindo a estrutura de pastas
    from scr.extract import extract_data
    from scr.transform.text import normalize_text_columns, normalize_text_data, clean_text_data
    from scr.transform.numeric import clean_numeric_data, fill_nan_numeric_data, round_metrics
    # Importando todas as validações
    from scr.validate import (
        validate_columns, validate_registers_count, nulls_year_column,
        validate_regions, validate_country_count, 
        generation_without_instaled_capacity, validate_composed_key
    )
    from scr.load import load_data
except ImportError as e:
    print(f"⚠️ Fallback de importação acionado: {e}")
    # Tentativa 2: Importação garantindo o config primeiro
    import config
    from scr.extract import extract_data
    from scr.transform.text import normalize_text_columns, normalize_text_data, clean_text_data
    from scr.transform.numeric import clean_numeric_data, fill_nan_numeric_data, round_metrics
    from scr.validation import (
        validate_columns, validate_registers_count, nulls_year_column,
        validate_regions, validate_country_count, 
        generation_without_instaled_capacity, validate_composed_key
    )
    from scr.load import load_data


# 3. Função Wrapper (Ponte)
def run_full_pipeline_dag():
    print("🚀 Iniciando Pipeline ETL no Airflow...")
    
    # 1. Extração
    df = extract_data()

    # 2. Transformações Textuais
    df = normalize_text_columns(df)
    df = normalize_text_data(df)
    df = clean_text_data(df)

    # 3. Transformações Numéricas
    df = clean_numeric_data(df)
    df = fill_nan_numeric_data(df)
    df = round_metrics(df)

    # 4. Validação
    print("🔍 Executando check de qualidade...")
    df = validate_columns(df)
    df = validate_registers_count(df)
    df = nulls_year_column(df)
    df = validate_regions(df)
    df = validate_country_count(df)
    df = generation_without_instaled_capacity(df)
    df = validate_composed_key(df)

    # 5. Carga Final
    print("💾 Carregando dados no Data Warehouse...")
    load_data(df)
    print("✅ Pipeline concluído com sucesso!")

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
        python_callable=run_full_pipeline_dag
    )

    etl_task