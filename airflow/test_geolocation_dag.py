# start pytest with the following command:


import pytest
import os
from sqlalchemy import create_engine, select, column, table
from dags.geolocation_dag import request_coordinates, write_initial_interesting_places

def test_request_coordinates():
    res = request_coordinates()
    assert "latitude" in res
    assert "longitude" in res
    assert isinstance(res["latitude"], float)
    assert isinstance(res["longitude"], float)

def test_write_initial_interesting_places():
    write_initial_interesting_places()
    conn = create_engine(os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/postgres")).connect()
    stmt = select(column('city')).select_from(table('places'))
    result = conn.execute(stmt).fetchall()
    cities = [row[0] for row in result]
    assert "Deuselbach" in cities
    assert "Grünstadt" in cities
    assert "Ludwigshafen" in cities
    assert "non-existing-city" not in cities