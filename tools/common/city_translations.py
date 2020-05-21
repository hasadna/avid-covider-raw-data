import tabulator
import json
import re
from pathlib import Path
from fuzzywuzzy import process, fuzz
import os
import dataflows as DF

from .helpers import data_file, all_data

DATA_FILE = data_file('cities_i18n.json')

def prepare():
    fp = re.compile('\w+', re.UNICODE)

    def fingerprint(x):
        return ''.join(fp.findall(x.upper()))

    langs, _, _ = DF.Flow(
        DF.load(all_data()),
        DF.concatenate(dict(
            city_name=[]
        ))
    ).results()
    langs = dict((k, dict()) for k in set(x['city_name'] for x in langs[0]))

    osm_he = {}
    s = tabulator.Stream(data_file('places.csv'))
    s.open()
    for item in s.iter():
        if len(item) == 0: continue
        item = json.loads(item[0])
        he = item.get('name:he', item.get('name'))
        if he is None:
            continue
        osm_he.setdefault(fingerprint(he), {}).update(item)

    s = tabulator.Stream(data_file('yeshuvim.csv'), headers=1)
    s.open()
    for item in s.iter(keyed=True):
        he = item['שם_ישוב'].strip().replace('(', 'XXX').replace(')', '(').replace('XXX', ')').replace('  ', ' ')
        en = item['שם_ישוב_לועזי'].strip().replace('  ', ' ').replace("'", 'xxx').title().replace('xxx', "'").replace('Xxx', "'")
        rec = {'name:en': en, 'name:he':he}
        he = fingerprint(he)
        osm_he.setdefault(he, {}).update(rec)

    osm = {}
    for item in osm_he.values():
        names = dict((k[5:], v) for k, v in item.items() if k.startswith('name:'))
        if 'name' not in item and 'he' not in names:
            continue
        names.setdefault('he', item.pop('name', None))
        for v in names.values():
            osm.setdefault(fingerprint(v), {}).update(names)
        en = names.get('en')
        if en and '-' in en:
            for v in en.split('-'):
                osm.setdefault(fingerprint(v), {}).update(names)

        old_names = dict(('he' if k=='old_name' else k[9:], v) for k, v in item.items() if k.startswith('old_name'))
        for v in old_names.values():
            for vv in v.split(';'):
                osm.setdefault(fingerprint(vv), {}).update(names)

    for kk, place in langs.items():
        k = fingerprint(kk)
        if k in osm:
            place.update(osm[k])
        else:
            match = process.extractOne(k, osm.keys(), scorer=fuzz.ratio)
            if match[1] >= 50:
                k = match[0]
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

