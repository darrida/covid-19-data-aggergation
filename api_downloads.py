# Standard
import csv
import json
import pprint

# PyPI
import requests

state = 'Illinois'

def api_death_weekly_by_state(state: str, year: int, week: int):
    url = f'https://data.cdc.gov/resource/muzy-jte6.json' \
        + f'?jurisdiction_of_occurrence={state}' \
        + f'&mmwryear={str(year)}' \
        + f'&mmwrweek={str(week)}'
    results = requests.get(url)
    return results.json()

total_difference = 0

try:
    for i in range(1, 52):
        data1 = api_death_weekly_by_state(state=state, year=2019, week=i)
        data2 = api_death_weekly_by_state(state=state, year=2020, week=i)
        difference = int(data2[0]['all_cause']) - int(data1[0]['all_cause'])
        total_difference += difference
        pprint.pprint(data1[0]['week_ending_date'] + ': ' + data1[0]['all_cause'] + ' => ' + data2[0]['week_ending_date'] + ': ' + data2[0]['all_cause'] + ' | Diff: ' + str(difference))
except IndexError as e:
    print(f'{i -1} is last week reported.')
    print(f'Total difference: {total_difference}')

# with open('death_weekly_by_stat.json', 'w') as outfile:
#     json.dump(data, outfile)