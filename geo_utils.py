import pandas as pd
from opencage.geocoder import OpenCageGeocode
import sys
import os
from dataclasses import dataclass
from unittest.mock import patch, MagicMock
import logging

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
        {'franchise_names': 'Franchise_1',
         'countries': 'Country_1',
         'cities': 'City_1'}
    ]
    pdf = pd.DataFrame(weather_data)
    with patch('geopy.geocoders.OpenCage') as mock_open_cage:
        mock_geocoder = MagicMock()
        mock_open_cage.return_value = mock_geocoder
        mock_geocoder.geocode.return_value = FakeLocation(1.0, -1.0)
        result = geocode_restaurant(pdf['granchise_names'], pdf['countries'], pdf['cities'])
        
        
def geocode_restaurant(franchise_names: pd.Series, countries: pd.Series, cities: pd.Series) -> pd.DataFrame:
    from geopy.geocoders import OpenCage
    
    latitudes = []
    longitudes = []
    geocoder = OpenCage(api_key=OPENCAGE_API_KEY)
    for franchise_name, country, city in zip(franchise_names, countries, cities):
        query = f'{franchise_name}, {city}'
        location = geocoder.geocode(query=f'{franchise_name}, {city}', country=country)
        if location is None:
            logger.warning(f'Geocoding no possible for "{query}"')
            continue
        latitudes.append(location.latitude)
        longitudes.append(location.longitude)
    return pd.DataFrame({'lat': latitudes, 'lng': longitudes})

def calculate_geohash(latitudes: pd.Series, longitudes: pd.Series) -> pd.Series:
    from geolib import geohash
    
    geohashes = []
    for latitude, longitude in zip(latitudes, longitudes):
        geohashes.append(geohash.encode(latitude, longitude, precision=4))
    return pd.Series(geohashes)

def haversine_distance(
    latitudes_x: pd.Series, longitudes_x: pd.Series, latitudes_y: pd.Series, longitudes_y: pd.Series
) -> pd.Series:
    from haversine import haversine
    
    distances = []
    for lat_x, lng_x, lat_y, lng_y in zip(latitudes_x, longitudes_y, latitudes_y, longitudes_y):
        distances.append(haversine((lat_x, lng_x), (lat_y, lng_y)))
    return pd.Series(distances)


