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
CUTOFF_LOW = 20
CUTOFF_LOW_CITIES = 40
CUTOFF_HIGH = 200

def fill_color(city, num_reports, sr, city_reports):
    if num_reports < CUTOFF_LOW:
        return (None, 'no-fill')
    elif num_reports < CUTOFF_LOW_CITIES:
        if city:
            return (None, 'no-fill')
        elif city_reports < CUTOFF_LOW_CITIES:
            return (None, 'no-fill')
    for level, color in COLOR_SCALE:
        if sr < level:
            return (color, 'band-below-{}'.format(level))
    return (color, 'band-below-{}'.format(level))

def pattern(city, num_reports, sr, city_reports):
    if num_reports >= CUTOFF_HIGH:
        return ('none', 'conf-high')
    else:
        return (None, 'conf-low')

def props():
    def func(rows):
        for row in rows:
            for k, f in [('fill', fill_color), ('pattern', pattern)]:
                r = copy.copy(row)
                r['kind'] = k
                r['property'], r['desc'] = f(r['is_city'] == 1, r['num_reports_weighted'], r['symptoms_ratio_weighted'], r['num_city_reports'])
                yield r
    return func

if __name__ == '__main__':

    city_fill_color_cases = ['case']
    city_fill_pattern_cases = ['case']
    neighborhood_fill_color_cases = ['case']
    neighborhood_fill_pattern_cases = ['case']

    r, _, _ = DF.Flow(
        DF.load(latest_file(), name='cities', cast_strategy=DF.load.CAST_WITH_SCHEMA),
        DF.filter_rows(lambda r: r['is_city']),
        DF.load(latest_file(), name='out', cast_strategy=DF.load.CAST_WITH_SCHEMA),
        DF.join('cities', ['city_name'], 'out', ['city_name'], dict(
            num_city_reports=dict(name='num_reports_weighted')
        )),
        DF.add_field('desc', 'string', ''),
        DF.add_field('kind', 'string', ''),
        DF.add_field('property', 'string', ''),
        props(),
        DF.join_with_self('out', ['is_city', 'kind', 'desc', 'property'],
                          dict(is_city=None, kind=None, desc=None, property=None, id=dict(aggregate='array'))),
    ).results()

    for item in r[0]:
        print('bucket for {} {} {}: {}'.format(
            'city' if item['is_city'] else 'neighborhood', item['kind'], item['desc'], len(item['id'])
        ))
        if item['property'] is None: 
            continue
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
    
