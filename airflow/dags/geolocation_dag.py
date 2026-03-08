# importing geopy library and Nominatim class
from geopy.geocoders import Nominatim
import pendulum
import requests
import os
from sqlalchemy import (
    create_engine, MetaData, Table,
    Column, Integer, String, Float
)

from sqlalchemy.dialects.postgresql import insert
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

def write_initial_interesting_places():
    interesting_places = [
        {"id": 1, "city": "Deuselbach"},
        {"id": 2, "city": "Grünstadt"},
        {"id": 3, "city": "Ludwigshafen"},
    ]
   

    # connection URL – adjust user/host/db/password as needed
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/postgres")

    engine = create_engine(DATABASE_URL)
    metadata = MetaData()

    places = Table(
        "places",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("city", String),
        Column("street", String),
        Column("number", String),
        Column("postal_code", String),
        Column("state", String),
        Column("country", String),
        Column("latitude", Float),
        Column("longitude", Float),
    )

    # create the table in the database if it does not yet exist
    metadata.create_all(engine)    # issues CREATE TABLE IF NOT EXISTS ...

    stmt = insert(places).values(interesting_places)
    update_cols = {
        c.name: getattr(stmt.excluded, c.name)
        for c in places.c
        if c.name != "id"
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_=update_cols,
    )

    with engine.begin() as conn:
        conn.execute(stmt)

def request_coordinates():
    # calling the Nominatim tool and create Nominatim class
    loc = Nominatim(user_agent="Geopy Library")

    # entering the location name
    getLoc = loc.geocode("Deuselbach")

    # printing address
    print(getLoc.address)

    # printing latitude and longitude
    print("Latitude = ", getLoc.latitude, "\n")
    print("Longitude = ", getLoc.longitude)
    return {"latitude": getLoc.latitude, "longitude": getLoc.longitude}

#def get_water_rlp():
#    url = "https://search.rlp-umwelt.de/haus?query=Grünstadt Oberer Bergelweg 7"
#    #https://geodaten-wasser.rlp-umwelt.de/api/data/twist_stammdaten?w=messst_nr=2391695021
#    #https://wasserportal.rlp-umwelt.de/auskunftssysteme/trinkwasserinformationssystem
#    response = requests.get(url)
#    data = response.json()
#    print(data)

with DAG(
    dag_id="geolocation_dag",
    start_date=pendulum.datetime(2016, 1, 1),
    schedule="@daily",
    default_args={"retries": 2},
):
   op = PythonOperator(task_id="write_initial_interesting_places", python_callable=write_initial_interesting_places)
   op = PythonOperator(task_id="request_coordinates", python_callable=request_coordinates)