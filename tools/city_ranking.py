import json
import dataflows as DF
import time
from common import all_data, city_translations, upload_file
from city_images import upload_static_image

def ranker():
    def func(rows):
        for i, r in enumerate(rows):
            r['rank'] = i + 1
            yield r
    return func

def sort_limit_scores():
    def func(row):
        row['scores'] = sorted(row.get('scores', []), key=lambda r: r['date'])[-30:]
    return func

if __name__ == '__main__':

    r, _, _ = DF.Flow(
        DF.load(all_data(), name='cities', headers=1,
                override_fields=dict(area_id=dict(type='string')),
                cast_strategy=DF.load.CAST_WITH_SCHEMA),
        DF.filter_rows(lambda r: r['is_city']),
        DF.add_field('score_date', 'object', lambda r: dict(
            date=r['date'].isoformat(), sr=float(r['symptoms_ratio_weighted'] or 0), nr=int(r['num_reports_weighted']))
        ),
        DF.concatenate(dict(
            id=[], city_name=[], score_date=[]
        ), target=dict(name='ranking')),
        DF.join_with_self('ranking', '{city_name}', dict(
            id=None, city_name=None, scores=dict(name='score_date', aggregate='array')
        )),
        sort_limit_scores(),
        DF.filter_rows(lambda r: r['scores'][-1]['nr'] >= 200),
        DF.add_field('sortkey', 'integer', lambda r: int(r['scores'][-1]['sr'] * 1000000) + r['scores'][-1]['nr']),
        DF.sort_rows('{sortkey}', reverse=True),
        DF.delete_fields(['sortkey']),
        DF.add_field('rank', 'integer', 0),
        DF.add_field('translations', 'object', lambda r: city_translations[r['city_name']]),
        DF.add_field('image', 'object', lambda r: upload_static_image(r['id'], width=280*2, height=160*2)),
        ranker(),
    ).results()
    rankings = r[0]

    r, _, _ = DF.Flow(
        DF.load(all_data(), name='cities', headers=1,
                override_fields=dict(area_id=dict(type='string')),
                cast_strategy=DF.load.CAST_WITH_SCHEMA),
        DF.filter_rows(lambda r: r['is_city']),
        DF.filter_rows(lambda r: r['num_reports_weighted'] >= 200),
        DF.add_field('ws', 'number', lambda r: r['symptoms_ratio_weighted'] * r['num_reports_weighted']),
        DF.concatenate(dict(
            date=[], num_reports_weighted=[], ws=[]
        ), target=dict(name='ranking')),
        DF.join_with_self('ranking', '{date}', dict(
            date=None, nr=dict(name='num_reports_weighted', aggregate='sum'), ws=dict(name='ws', aggregate='sum')
        )),
        DF.add_field('sr', 'number', lambda r: r['ws']/r['nr']),
        DF.delete_fields(['ws']),
        DF.sort_rows('{date}'),
    ).results()

    national = dict(
        id='NATIONAL', rank=0, scores=[
            dict(nr=rr['nr'], sr=float(rr['sr']), date=rr['date'].isoformat())
            for rr in r[0]
        ][-30:]
    )
    rankings.insert(0, national)
    upload_file(json.dumps(rankings).encode('utf8'), 'data/city_rankings.json')

