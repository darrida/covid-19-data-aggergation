# Standard
import csv
import json
import pprint

# PyPI
import requests
import pandas

uri = 'https://data.cdc.gov/resource/muzy-jte6.json'
state = 'Georgia'
output = 'death_weekly_by_state.json'


def api_death_weekly_by_state(state: str, year: int, week: int, uri=uri):
    url = uri \
        + f'?jurisdiction_of_occurrence={state}' \
        + f'&mmwryear={str(year)}' \
        + f'&mmwrweek={str(week)}'
    results = requests.get(url)
    return results.json()



def _api_death_compare(state: str, year1: int, year2: int):
    totals = {
        'year1': 0,
        'year2': 0,
        'difference': 0,
        'total_diff': 0
    }
    try:
        for i in range(1, 52):
            data1 = api_death_weekly_by_state(state=state, year=year1, week=i)
            data2 = api_death_weekly_by_state(state=state, year=year2, week=i)
            #difference = int(data2[0]['all_cause']) - int(data1[0]['all_cause'])
            totals['difference'] = int(data2[0]['all_cause']) - int(data1[0]['all_cause'])
            totals['total_diff'] += int(totals['difference'])
            totals['year1'] += int(data1[0]['all_cause'])
            totals['year2'] += int(data2[0]['all_cause'])
            #total_difference += difference
            # print(
            #     data1[0]['week_ending_date'], ':', data1[0]['all_cause'], '=>', data2[0]['week_ending_date'] + ': ' + data2[0]['all_cause'], 
            #     '| TOTALS - year1:', totals['year1'], '- year2:', totals['year2'],
            #     '| Diff:', totals['difference']
            # ) 
    except IndexError:
        print(f'{i -1} is last week reported.')
        total_diff = totals['total_diff']
        print(f'Total difference: {total_diff}')


def api_death_all_data_json(output_name_location: str, uri=uri):
    results = requests.get(uri)
    data = results.json()
    with open(output_name_location, 'w') as outfile:
        json.dump(data, outfile)


def json_to_csv(json_input: str):
    df = pandas.read_json(json_input)
    df.to_csv('death_weekly_by_state.csv')

_api_death_compare('Illinois', 2019, 2020)
