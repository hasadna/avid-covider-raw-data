import glob
import os
import logging
from io import BytesIO

import boto3
from botocore.exceptions import ClientError

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
data_dir = os.path.join(root_dir, 'input')
geo_dir = os.path.join(root_dir, 'geo')


def latest_file():
    return sorted(glob.glob(os.path.join(data_dir, '*.csv')))[-1]

def geo_file(name):
    return os.path.join(geo_dir, name + '.geojson')

def upload_file(data, object_name):
    buckets = ['avid-covider.phonaris.com', 'coronaisrael.org']
    
    # Upload the file
    s3_client = boto3.client('s3', region_name='eu', endpoint_url='https://storage.googleapis.com',
                    aws_access_key_id=os.environ['AWS_ACCESS_KEY'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    for bucket_name in buckets:
        try:
            f = BytesIO(data)
            response = s3_client.upload_fileobj(f, bucket_name, object_name, ExtraArgs={'ACL': 'public-read'})
        except ClientError as e:
            logging.error(e)
            return False
    return True

    

