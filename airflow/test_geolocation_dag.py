# start pytest with the following command:


import pytest
from dags.geolocation_dag import request_coordinates

def test_request_coordinates():
    res = request_coordinates()
    assert "latitude" in res
    assert "longitude" in res
    assert isinstance(res["latitude"], float)
    assert isinstance(res["longitude"], float)