import os
import requests
import logging

def mapbox_api_key():
    return os.environ['MAPBOX_API_KEY']

def upload_tileset(tileset_id, tileset_name, url):
    response = requests.post('https://api.mapbox.com/uploads/v1/wios', dict(
        tileset=tileset_id,
        url=url,
        name=tileset_name,
        access_token=mapbox_api_key(),
    ))
    logging.info('UPLOAD Tileset %s (%s) @ %s:\n%s', tileset_id, tileset_name, url, response.text)

