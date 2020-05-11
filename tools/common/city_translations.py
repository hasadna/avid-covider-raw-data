import tabulator
import json
import re
from pathlib import Path
from fuzzywuzzy import process, fuzz
import os
import dataflows as DF

from .helpers import data_file, all_input_files

DATA_FILE = data_file('cities_i18n.json')

def prepare():
    fp = re.compile('\w+', re.UNICODE)

    def fingerprint(x):
        return ''.join(fp.findall(x.upper()))

    langs, _, _ = DF.Flow(
        *[
            DF.load(f)
            for f in all_input_files()
        ],
        DF.concatenate(dict(
            city_name=[]
        ))
    ).results()
    langs = dict((k, dict(en=k)) for k in set(x['city_name'] for x in langs[0]))

    osm = {}
    s = tabulator.Stream(data_file('places.csv'))
    s.open()
    for item in s.iter():
        if len(item) == 0: continue
        item = json.loads(item[0])
        item = dict(('he' if k=='name' else k[5:], v) for k, v in item.items() if k.startswith('name'))
        for v in item.values():
            osm[fingerprint(v)] = item

    s = tabulator.Stream(data_file('yeshuvim.csv'), headers=1)
    s.open()
    for item in s.iter(keyed=True):
        he = item['שם_ישוב'].strip().replace('(', 'XXX').replace(')', '(').replace('XXX', ')').replace('  ', ' ')
        en = item['שם_ישוב_לועזי'].strip().replace('  ', ' ').replace("'", 'xxx').title().replace('xxx', "'").replace('Xxx', "'")
        rec = dict(en=en, he=he)
        osm.setdefault(he, {}).update(rec)
        for p in en.split('-'):
            osm.setdefault(p, {}).update(rec)
        osm.setdefault(en, {}).update(rec)

    for place in langs.values():
        k = fingerprint(place['en'])
        if k in osm:
            osm[k].update(place)
            place.update(osm[k])
        else:
            match = process.extractOne(k, osm.keys(), scorer=fuzz.ratio)
            if match[1] >= 50:
                k = match[0]
                osm[k].update(place)
                place.update(osm[k])
            else:
                print('no match for', place, k, match)

    normalized = dict(
        (k, dict(
            he=v.get('he'),
            en=v.get('en'),
            es=v.get('es', v.get('en')),
            ru=v.get('ru', v.get('en')),
            ar=v.get('ar', v.get('he'))
        )) for k, v in langs.items()
    )
    json.dump(normalized, open(DATA_FILE, 'w'))

if not os.path.exists(DATA_FILE):
    prepare()

city_translations = json.load(open(DATA_FILE))

