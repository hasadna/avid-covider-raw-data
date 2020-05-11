import json
import dataflows as DF
from common import latest_week_files, city_translations, upload_file

def ranker():
    def func(rows):
        for i, r in enumerate(rows):
            r['rank'] = i + 1
            yield r
    return func

if __name__ == '__main__':

    r, _, _ = DF.Flow(
        *[
            DF.load(f, name='cities', headers=1, cast_strategy=DF.load.CAST_WITH_SCHEMA)
            for f in latest_week_files()
        ],
        DF.filter_rows(lambda r: r['is_city']),
        DF.add_field('score_date', 'object', lambda r: dict(
            date=r['date'].isoformat(), sr=float(r['symptoms_ratio_weighted']), nr=int(r['num_reports_weighted']))
        ),
        DF.concatenate(dict(
            city_name=[], score_date=[]
        ), target=dict(name='ranking')),
        DF.join_with_self('ranking', '{city_name}', dict(
            city_name=None, scores=dict(name='score_date', aggregate='array')
        )),
        DF.filter_rows(lambda r: r['scores'][-1]['nr'] >= 200),
        DF.add_field('sortkey', 'number', lambda r:  r['scores'][-1]['sr']),
        DF.sort_rows('{sortkey}', reverse=True),
        DF.delete_fields(['sortkey']),
        DF.add_field('rank', 'integer', 0),
        DF.add_field('translations', 'object', lambda r: city_translations[r['city_name']]),
        ranker(),
    ).results()

    rankings = r[0]

    upload_file(json.dump(rankings).encode('utf8'), 'data/city_rankings.json')

