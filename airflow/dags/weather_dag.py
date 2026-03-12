# importing geopy library and Nominatim class
from geopy.geocoders import Nominatim
import pendulum
import requests
import os
from sqlalchemy import (
    create_engine, MetaData, Table,
    Column, Integer, String, Float, Select
)

from sqlalchemy.dialects.postgresql import insert
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

def get_weather_data():
    ## gets current weather data from openweathermap for the cities in the places table and writes it to the weather table
    # get places from database
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/postgres")
    engine = create_engine(DATABASE_URL)
    stmt = Select(Table('places', MetaData()).c["city", "id"])
    with engine.connect() as conn:
        result = conn.execute(stmt)
        places = result.fetchall()
    # for each place, get weather data and write to database
    weather={}
    for place in places:
        city = place["city"]    
        response = requests.get("https://api.openweathermap.org/data/2.5/weather?q=" + city + "&appid=bfa574ef4628eb6657839045988f7d55")
        data = response.json()
        weather[city] = data

    return weather

# weather data
with DAG(
    dag_id="weather_dag",
    start_date=pendulum.datetime(2016, 1, 1),
    schedule="@hourly",
    default_args={"retries": 2},
):
   op1 = PythonOperator(task_id="get_weather_data", python_callable=get_weather_data)
   op1
   