from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
import requests
from requests.auth import HTTPBasicAuth
import duckdb
import json

# Variable globale en MAJ
COL_OPEN_SKY = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category"
]

URL_ALL_STATES = 'https://opensky-network.org/api/states/all?extended=true'
CREDS_OPEN_SKY = HTTPBasicAuth('CVincent', 'm2umEMdHbF93Bu3')
DATA_FILE_NAME = 'dags/data/data.json'

@task
def get_flight_data(col, url, creds, data_file_name):
    req = requests.get(URL_ALL_STATES, auth=creds)
    # raise for status lève une exception si l'api ne retourne pas le code 200
    req.raise_for_status()
    resp = req.json()
    timestamp = resp['time']
    states_list = resp['states']
    states_json = [dict(zip(col, state)) for state in states_list]
    with open(DATA_FILE_NAME, 'w', encoding='utf-8') as f:
        json.dump(states_json, f)

@task
def load_from_file(data_file_name):
    conn = None
    try:
        conn = duckdb.connect('dags/data/bdd_airflow')
        conn.sql(f"INSERT INTO bdd_airflow.main.openskynetwork_brute (SELECT * FROM '{data_file_name}')")
    
    except Exception as e:
        print(e)
    
    finally:
        if conn:
            conn.close()   

@task
def check_row_numbers():
    conn = None
    nbr_rows = 0
    try:
        conn = duckdb.connect('dags/data/bdd_airflow', read_only=True)
        # fetchone permet de récup la valeur de nbr_rows et [0] car c'est un tuple : (22043,) et on veut la 1ere valeur
        nbr_rows = conn.sql(f"SELECT COUNT(*) FROM bdd_airflow.main.openskynetwork_brute").fetchone()[0]
    finally:
        if conn:
            conn.close()   
    print(f"Nombre de lignes: {nbr_rows}")

@task
def check_duplicates():
    conn = None
    nbr_dupli = 0
    try:
        conn = duckdb.connect('dags/data/bdd_airflow', read_only=True)
        nbr_dupli = conn.sql("""
        SELECT callsign, time_position, last_contact, count(*) as cnt
        FROM bdd_airflow.main.openskynetwork_brute
        GROUP BY 1,2,3
        HAVING cnt > 1;                 
        """).count(column='cnt').fetchone()[0]
    finally:
        if conn:
            conn.close()   
    print(f"Nombre de duplications: {nbr_dupli}")

@dag()
def flights_pipeline():
    (
        EmptyOperator(task_id="start")
        >> get_flight_data(COL_OPEN_SKY, URL_ALL_STATES, CREDS_OPEN_SKY, DATA_FILE_NAME)
        >> load_from_file(DATA_FILE_NAME)
        # [] pour tâches en parallèles
        >> [check_row_numbers(), check_duplicates()]
        >> EmptyOperator(task_id="end")
        # >> = hiérarchie entre les taches (ici end doit s'exec après start)
    )

flights_pipeline_dag = flights_pipeline()