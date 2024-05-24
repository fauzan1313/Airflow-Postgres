import psycopg2
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime
import subprocess

def _install_dependencies():
    subprocess.run(["pip","install","numpy"])
    subprocess.run(["pip","install","psycopg2"])

# samakan dengan yg ada di docker-compose.yaml
def _connect_db():
    conn = psycopg2.connect(database="postgres",user="user", password="Password@9876",
                            host="0.0.0.0", port="5433")
    conn.close()
    print('DB Sukses Terkoneksi')

def _create_table_db():
    conn = psycopg2.connect(database="postgres",user="user", password="Password@9876",
                            host="0.0.0.0", port="5433")
    conn.autocommit=True
    cursor = conn.cursor()
    create_table = '''
                CREATE TABLE if not exist wine_data 
                (type VARCHAR,
                fixed_acidity FLOAT,
                volatile_acidity FLOAT,
                citric_acid FLOAT,
                residual_sugar FLOAT,
                chlorides FLOAT,
                free_sulfur_dioxide INT,
                total_sulfur_dioxide INT,
                density FLOAT,
                pH FLOAT,
                sulphates FLOAT,
                alcohol FLOAT,
                quality INT)
                '''
    cursor.execute(create_table)
    conn.close()
    print("Table telah terbuat")

def _copy_csv_table():
    conn = psycopg2.connect(database="postgres",user="user", password="Password@9876",
                            host="0.0.0.0", port="5433")
    conn.autocommit=True
    cursor = conn.cursor()
    copy_csv = '''
                COPY wine_data (type, fixed_acidity, volatile_acidity, citric_acid, residual_sugar, chlorides,
                free_sulfur_dioxide, total_sulfur_dioxide, density, pH, sulphates, alcohol, quality)
                FROM '/opt/airflow/dags/data_cleaned.csv'
                DELIMITER ','
                CSV HEADER;
                '''
data_ingestion_dag = DAG(dag_id='data-ingestion-csv2pg',
                        description='Data Ingestion DAG from CSV to PostgreSQL DB',
                        schedule_interval='@daily',
                        catchup=False,
                        start_date=datetime(2024,5,24))
#define tasks
install_dependencies = PythonOperator(
    task_id = "install_dependencies",
    python_callable=_install_dependencies,
    dag=data_ingestion_dag
)

data_cleaning = BashOperator(
    task_id = "data-cleaning",
    bash_command='python /opt/airflow/dags/data_cleaning.py', # ADA SPACE ANTARA PYTHON dan DIRECTORY
    dag=data_ingestion_dag
)

connect_db = PythonOperator(
    task_id = "connect_db",
    python_callable=_connect_db,
    dag=data_ingestion_dag
)

create_table_db = PythonOperator(
    task_id = "create_table_db",
    python_callable=_create_table_db,
    dag=data_ingestion_dag
)

copy_csv_tabel = PythonOperator(
    task_id = "copy_csv_table",
    python_callable=_copy_csv_table,
    dag=data_ingestion_dag
)

install_dependencies >> data_cleaning >> connect_db >> create_table_db >>copy_csv_tabel