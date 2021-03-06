import json
import decimal
import os
import logging
import dataflows as DF
from common import geo_file, latest_file, upload_file, upload_tileset, json_encoder



def process_file(key, is_city, filename, latest):
    gj = json.load(open(filename))
    default = dict(
        latest_confidence = 0,
        latest_ratio = 0,
        latest_reports = 0,
        population = 0
    )
    for feature in gj['features']:
        properties = feature['properties']
        properties.update(
            latest.get((properties['id'], is_city), default)
        )
    upload = json.dumps(gj, cls=json_encoder).encode('utf8')
    path = 'data/tilesets/static-images-{}.geojson'.format(key)
    logging.info('UPLOADING %d bytes to %s', len(upload), path)
    upload_file(upload, path)
    upload_tileset(upload, 'static-images-' + key)


if __name__ == '__main__':
    data, _, _ = DF.Flow(
        DF.load(latest_file(), name='out', 
                override_fields=dict(area_id=dict(type='string')),
                cast_strategy=DF.load.CAST_WITH_SCHEMA),
        DF.concatenate(dict(
            id=[],
            is_city=[],
            latest_confidence=['symptoms_ratio_confidence_weighted'],
            latest_ratio=['symptoms_ratio_weighted'],
            latest_reports=['num_reports_weighted'],
            population=[]
        ))
    ).results()
    data = dict(
        ((r.pop('id'), r.get('is_city')), r) for r in data[0]
    )
    for key, is_city in [('cities', 1), ('neighborhoods', 0)]:
        process_file(key, is_city, geo_file(key), data)