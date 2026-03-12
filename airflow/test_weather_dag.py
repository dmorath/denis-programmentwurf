# start pytest with the following command:


import pytest
import os
from sqlalchemy import create_engine, select, column, table
from dags.weather_dag import get_weather_data

def test_get_weather_data():
    res = get_weather_data()
    assert isinstance(res, dict)

