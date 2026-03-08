# importing geopy library and Nominatim class
from geopy.geocoders import Nominatim
import pendulum
import requests

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

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
   op = PythonOperator(task_id="request_coordinates", python_callable=request_coordinates)