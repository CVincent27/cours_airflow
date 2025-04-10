import duckdb
import json
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

COLONNES_OPEN_SKY = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude"
]

URL_ALL_STATES = 'https://opensky-network.org/api/states/all?extended=true'
CREDENTIALS = HTTPBasicAuth('CVincent', 'm2umEMdHbF93Bu3')
DATA_FILE_NAME = 'dags/data/data.json'

@task()
def get_flight_data(col, url, creds, data_file_name):
    req = requests.get(url, auth=creds)
    req.raise_for_status()
    resp = req.json()
    timestamp = resp['time']
    states_list = resp['states']
    states_json = [dict(zip(col, state)) for state in states_list]
    with open(data_file_name, 'w') as f:
        json.dump(states_json, f)

@task()
def load_from_file(data_file_name):
    conn = None
    try:
        conn = duckdb.connect('dags/data/bdd_airflow')
        conn.sql(f"INSERT INTO bdd_airflow.main.openskynetwork_brute (SELECT * FROM '{data_file_name}')") 
    finally:
        if conn:
            conn.close()

@task()
def check_row_number():
    conn = None
    nbr_rows = 0
    try:
        conn = duckdb.connect('dags/data/bdd_airflow', read_only=True)
        result = conn.execute("SELECT COUNT(*) FROM bdd_airflow.main.openskynetwork_brute")
        nbr_rows = result.fetchone()[0]
    finally:
        if conn:
            conn.close()
    print(f"Nombre de lignes dans la table : {nbr_rows}")

@task()
def check_duplicates():
    conn = None
    nbr_duplicates = 0
    try:
        conn = duckdb.connect('dags/data/bdd_airflow', read_only=True)
        nbr_duplicates = conn.sql("""
        SELECT callsign, time_position, last_contact, count(*) AS cnt
        FROM bdd_airflow.main.openskynetwork_brute
        GROUP BY 1,2,3 
        HAVING cnt > 1
        """).count(column="cnt").fetchone()[0]
    finally:
        if conn:
            conn.close()
    print(f"Nombre de doublons dans la table : {nbr_duplicates}")

@dag(
    dag_id='flights_pipeline',  # Assurez-vous de définir un ID pour votre DAG
    start_date=datetime(2023, 4, 10),  # Définissez une date de début
    schedule_interval=None  # Pour exécuter le DAG manuellement (ou définissez un intervalle de planification)
)
def flights_pipeline():
    (
        EmptyOperator(task_id="start")
        >> get_flight_data(COLONNES_OPEN_SKY, URL_ALL_STATES, CREDENTIALS, DATA_FILE_NAME)
        >> load_from_file(DATA_FILE_NAME)
        >> [check_row_number(), check_duplicates()]
        >> EmptyOperator(task_id="end")
    )

# Exécute le DAG
flight_pipeline_dag = flights_pipeline()
