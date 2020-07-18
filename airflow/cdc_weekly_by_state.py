import json
import pathlib
import pprint
import csv
#import pandas

import airflow
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator #bash_operator
from airflow.operators.python_operator import PythonOperator #python_operator

dag = DAG(
    dag_id="download_cdc_morality_weekly",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily")


uri = 'https://data.cdc.gov/resource/muzy-jte6.json'
state = 'Georgia'
output = 'death_weekly_by_state.json'

# download_launches = BashOperator(
#     task_id="download_",
#     bash_command="curl -o /tmp/launches.json 'https://launchlibrary.net/1.4/launch?next=5&mode=verbose'",
#     dag=dag,
# )

def api_death_weekly_by_state(state: str, year: int, week: int, uri=uri):
    url = uri \
        + f'?jurisdiction_of_occurrence={state}' \
        + f'&mmwryear={str(year)}' \
        + f'&mmwrweek={str(week)}'
    results = requests.get(url)
    return results.json()



def _api_death_compare(**context):
    state = context["state"]
    year1 = context["year1"]
    year2 = context["year2"]
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
            print(
                data1[0]['week_ending_date'], ':', data1[0]['all_cause'], '=>', data2[0]['week_ending_date'] + ': ' + data2[0]['all_cause'], 
                '| TOTALS - year1:', totals['year1'], '- year2:', totals['year2'],
                '| Diff:', totals['difference']
            ) 
    except IndexError:
        print(f'{i -1} is last week reported.')
        total_diff = totals['total_diff']
        print(f'Total difference: {total_diff}')


# def api_death_all_data_json(output_name_location: str, uri=uri):
#     results = requests.get(uri)
#     data = results.json()
#     with open(output_name_location, 'w') as outfile:
#         json.dump(data, outfile)


# def json_to_csv(json_input: str):
#     df = pandas.read_json(json_input)
#     df.to_csv('death_weekly_by_state.csv')

get_cdc_mortality_weekly = PythonOperator(
    task_id="download_cdc_moraltiy_weekly",
    python_callable=_api_death_compare,
    op_kwargs={
        "state": "Illinois",
        "year1": 2019,
        "year2": 2020,
    },
    provide_context=True,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "cdc data downloaded."',
    dag=dag
)

get_cdc_mortality_weekly >> notify