import json
import copy

import dataflows as DF
import tabulator

from common import latest_file, upload_file

COLOR_SCALE = [
    [1, '#fae700'],
    [2, '#ffca5d'],
    [3, '#ffad88'],
    [4, '#ff8caf'],
    [5, '#ff5fd8'],
    [1000, '#f700ff'],
]
CUTOFF_LOW = 40
CUTOFF_HIGH = 200

def fill_color(city, num_reports, sr):
    if num_reports < CUTOFF_LOW:
        return None
    for level, color in COLOR_SCALE:
        if sr < level:
            return color
    return color

def pattern(city, num_reports, sr):
    if num_reports >= CUTOFF_HIGH:
        return 'none'
    else:
        return None

def props():
    def func(rows):
        for row in rows:
            for k, f in [('fill', fill_color), ('pattern', pattern)]:
                r = copy.copy(row)
                r['kind'] = k
                r['property'] = f(r['is_city'] == 1, r['num_reports_weighted'], r['symptoms_ratio_weighted'])
                yield r
    return func

if __name__ == '__main__':

    city_fill_color_cases = ['case']
    city_fill_pattern_cases = ['case']
    neighborhood_fill_color_cases = ['case']
    neighborhood_fill_pattern_cases = ['case']

    r, _, _ = DF.Flow(
        DF.load(latest_file(), name='out', cast_strategy=DF.load.CAST_WITH_SCHEMA),
        DF.add_field('kind', 'string', ''),
        DF.add_field('property', 'string', ''),
        props(),
        DF.filter_rows(lambda r: r['property'] is not None),
        DF.join_self('out', ['is_city', 'kind', 'property'], 'out', dict(is_city=None, kind=None, property=None, id=dict(aggregate='array'))),
    ).results()

    for item in r[0]:
        if item['kind'] == 'fill':
            if item['is_city']:
                city_fill_color_cases.extend([
                ['in', ['get', 'id'], ['literal', item['id']]], item['property']
                ])
            else:
                neighborhood_fill_color_cases.extend([
                ['in', ['get', 'id'], ['literal', item['id']]], item['property']
                ])
        else:
            if item['is_city']:
                city_fill_pattern_cases.extend([
                ['in', ['get', 'id'], ['literal', item['id']]], ['image', item['property'].replace('none', '')]
                ])
            else:
                neighborhood_fill_pattern_cases.extend([
                ['in', ['get', 'id'], ['literal', item['id']]], ['image', item['property'].replace('none', '')]
                ])

    city_fill_color_cases.append('rgba(0,0,0,0)')
    neighborhood_fill_color_cases.append('rgba(0,0,0,0)')
    city_fill_pattern_cases.append(['image', 'pattern-4'])
    neighborhood_fill_pattern_cases.append(['image', 'pattern-4'])

    update_date = (next(tabulator.Stream(latest_file(), headers=1).open().iter(keyed=True)))['date']

    out = dict(
        city_fill_color_cases=city_fill_color_cases,
        city_fill_pattern_cases=city_fill_pattern_cases,
        neighborhood_fill_color_cases=neighborhood_fill_color_cases,
        neighborhood_fill_pattern_cases=neighborhood_fill_pattern_cases,
        update_date=update_date,
        color_scale=COLOR_SCALE,
        cutoff_low=CUTOFF_LOW,
        cutoff_high=CUTOFF_HIGH,
    )
    out = json.dumps(out, sort_keys=True)
    upload_file(out.encode('utf8'), 'data/map_coloring.json')
    
