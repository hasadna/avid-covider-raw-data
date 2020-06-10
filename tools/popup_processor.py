import json
import dataflows as DF
import time
from common import all_data, city_translations, upload_file
from city_images import upload_static_image

def sort_limit_scores():
    def func(row):
        row['scores'] = sorted(row.get('scores', []), key=lambda r: r['date'])[-28:]
    return func

def split_to_weeks():
    def func(row):
        scores = row['scores']
        weeks = []
        week = []
        for i in scores:
            if i['weekday'] == 0:
                if len(week) > 0:
                    weeks.append(week)
                    week = []
            week.append(i)
            assert len(week) <= 7
        if len(week) > 0:
            weeks.append(week)
            week = []
        assert len(weeks) <= 5
        if len(weeks) == 4:
            weeks.insert(0, [
                dict(weekday=i) for i in range(7)
            ])
        else:
            while weeks[0][0]['weekday'] > 0:
                weeks[0].insert(0, dict(
                    weekday=weeks[0][0]['weekday'] - 1
                ))
            while weeks[-1][-1]['weekday'] < 6:
                weeks[-1].append(dict(
                    weekday=weeks[-1][-1]['weekday'] + 1
                ))
        assert all(len(x) == 7 for x in weeks)
        row['scores'] = weeks
    return func

if __name__ == '__main__':

    r, _, _ = DF.Flow(
        DF.load(all_data(), name='cities', headers=1,
                override_fields=dict(area_id=dict(type='string')),
                cast_strategy=DF.load.CAST_WITH_SCHEMA),
        DF.filter_rows(lambda r: r['is_city']),
        DF.add_field('score_date', 'object', lambda r: dict(
            weekday=r['date'].isoweekday() % 7, date=r['date'].isoformat(), sr=float(r['symptoms_ratio_weighted'] or 0), nr=int(r['num_reports_weighted']))
        ),
        DF.concatenate(dict(
            id=[], city_name=[], score_date=[]
        ), target=dict(name='popup_data')),
        DF.join_with_self('popup_data', '{city_name}', dict(
            id=None, city_name=None, scores=dict(name='score_date', aggregate='array')
        )),
        sort_limit_scores(),
        split_to_weeks(),
        DF.add_field('translations', 'object', lambda r: city_translations[r['city_name']]),
    ).results()
    popup_data = r[0]
    popup_data = dict((x['id'], x) for x in popup_data)

    upload_file(json.dumps(popup_data, indent=2).encode('utf8'), 'data/popup_data.json')

