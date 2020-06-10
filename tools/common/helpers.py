import glob
import os
import logging
import tempfile
import json
import decimal
from io import BytesIO
from mimetypes import guess_type

import boto3
from botocore.exceptions import ClientError

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
data_dir = os.path.join(root_dir, 'input')
geo_dir = os.path.join(root_dir, 'geo')
extra_data_dir = os.path.join(root_dir, 'data')


def all_input_files():
    return sorted(glob.glob(os.path.join(data_dir, '20*.csv')))

def latest_week_files():
    return all_input_files()[-7:]

def all_data():
    return os.path.join(data_dir, 'all_dates.csv')

def latest_file():
    return all_input_files()[-1]

def geo_file(name):
    return os.path.join(geo_dir, name + '.geojson')

def data_file(name):
    return os.path.join(extra_data_dir, name)

def upload_file(data, object_name):
    buckets = ['avid-covider.phonaris.com', 'coronaisrael.org']
    
    if 'AWS_ACCESS_KEY' not in os.environ:
      print('Skipping upload of {}'.format(object_name))
      with open(object_name, 'wb') as f:
          f.write(data)
      return True

    # Upload the file
    s3_client = boto3.client('s3', region_name='eu', endpoint_url='https://storage.googleapis.com',
                    aws_access_key_id=os.environ['AWS_ACCESS_KEY'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    for bucket_name in buckets:
        try:
            mimetype = guess_type(object_name)[0] or 'application/octet-stream'
            response = s3_client.put_object(Body=data, Bucket=bucket_name, Key=object_name,
                                            ACL='public-read', CacheControl='max-age=600',
                                            ContentType=mimetype)
            print('uploaded', object_name, 'to', bucket_name)
        except ClientError as e:
            logging.error(e)
            return False
    return True

class json_encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return JSONEncoder.default(self, o)
