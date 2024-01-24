import pandas as pd
from opencage.geocoder import OpenCageGeocode
import sys
import os
from dataclasses import dataclass
from unittest.mock import patch, MagicMock
import logging
from geo_utils import geocode_restaurant, calculate_geohash, haversine_distance
import quinn

logger = logging.getLogger(__name__)

OPENCAGE_API_KEY='c2258df36ebe43d7823bd7818c324ccf'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


@dataclass
class FakeLocation:
    longitude: float
    latitude: float

def test_geocode_restaurant() -> None:
    weather_data = [
        {'franchise_names': 'Cafe Crepe',
         'countries': 'IT',
         'cities': 'Milan'}
    ]
    pdf = pd.DataFrame(weather_data)
    with patch('geopy.geocoders.OpenCage') as mock_open_cage:
        mock_geocoder = MagicMock()
        mock_open_cage.return_value = mock_geocoder
        mock_geocoder.geocode.return_value = FakeLocation(1.0, -1.0)
        result = geocode_restaurant(pdf['franchise_names'], pdf['countries'], pdf['cities'])
        assert (result.loc[0]['lng'], result.loc[0]['lat']) == (1.0, -1.0)


def test_calculate_geohash() -> None:
    with patch('geo_utils.calculate_geohash') as mock_calculate_geohash:
        mock_calculate_geohash.return_value = 'ebpm'
        result = calculate_geohash([1.0], [-1.0])
        assert result[0] == 'ebpm'
        

def test_haversine_distance() -> None:
    with patch('geo_utils.haversine_distance') as mock_haversine_distance:
        mock_haversine_distance.return_value = 0.0
        result = haversine_distance([1.0], [-1.0], [1.0], [-1.0])
        assert result[0] == 0.0


if __name__ == '__main__':
    test_geocode_restaurant()
    test_calculate_geohash()
    test_haversine_distance()
