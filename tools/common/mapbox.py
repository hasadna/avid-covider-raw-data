import os
import requests
import logging
import boto3

def mapbox_api_key():
    return os.environ['MAPBOX_API_TOKEN']

def upload_tileset(data, tileset_id, tileset_name):
    creds = requests.post('https://api.mapbox.com/uploads/v1/wios/credentials?access_token={}'.format(mapbox_api_key())).json()
    s3_client = boto3.client('s3', region_name='us-east-1', 
                             aws_access_key_id=creds['accessKeyId'],
                             aws_secret_access_key=creds['secretAccessKey'])
    s3_client.put_object(Body=data, Bucket=creds['bucket'], Key=creds['key'])
    response = requests.post('https://api.mapbox.com/uploads/v1/wios?access_token={}'.format(mapbox_api_key()),
        json=dict(
            tileset=tileset_id,
            url=creds['url'],
            name=tileset_name,
        ))
    logging.info('UPLOAD Tileset %s (%s) @ %s:\n%s', tileset_id, tileset_name, url, response.text)

