from math import sin, cos, sqrt, atan2, radians, log
import json
import requests
from common import geo_file, upload_file

def distance(lat1, lon1, lat2, lon2):
    # approximate radius of earth in km
    R = 6373.0

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c * 1000.0

def center(lat1, lon1, lat2, lon2):
    return (lat1 + lat2)/2, (lon1 + lon2)/2

def extent(lats, lons):
    return (min(lats), min(lons), max(lats), max(lons))

def split_coordinates(coordinates):
    if not isinstance(coordinates[0], list):
        assert len(coordinates) == 2
        return [coordinates[1]], [coordinates[0]]
    else:
        conv = [split_coordinates(x) for x in coordinates]
        lats = []
        lons = []
        for lat, lon in conv:
            lats.extend(lat)
            lons.extend(lon)
        return lats, lons

def zoomlevel(size, lat1, lon1, lat2, lon2):
    c_lat, c_lon = center(lat1, lon1, lat2, lon2)
    dist = max(distance(c_lat, lon1, c_lat, lon2),
               distance(lat1, c_lon, lat2, c_lon))
    scaled = dist / cos(radians(c_lat))
    level = 78271.484 / scaled * size
    print(dist, scaled, level)
    return min(log(level)/log(2), 20)

def static_image_url(lat, lng, zoom, width, height):
    return f'https://api.mapbox.com/styles/v1/wios/ck9spo9jo02p71ip3m6xfchxk/static/' +\
           f'{lng},{lat},{zoom},0/{width}x{height}@2x' +\
           f'?access_token=pk.eyJ1Ijoid2lvcyIsImEiOiJjazh4ZXZ6Z24wejdtM3JvN2F1MHdlc2Z4In0.vz7knGcRWWGE4LGOLx8c7g'

def upload_static_image(id, width=600, height=400):
    features = json.load(open(geo_file('cities')))
    feature = next(filter(lambda f: f['properties']['id'] == id, features['features']))
    coords = split_coordinates(feature['geometry']['coordinates'])
    # print(feature)
    bbox = extent(*coords)
    ctr = center(*bbox)
    size = width, height
    zoom = zoomlevel(min(size) * 1.1, *bbox)
    path = 'data/city_preview_{}.png'.format(id)
    url = static_image_url(*ctr, zoom, *size)
    # upload_file(requests.get(url).content, path)
    return '/' + path