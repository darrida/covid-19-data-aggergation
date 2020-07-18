import json
import pathlib
import pprint
import csv
import pandas

import airflow
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator #bash_operator
from airflow.operators.python_operator import PythonOperator #python_operator

dag = DAG(
    dag_id="dl_cdc_morality_full_set",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily")


def _api_death_all_data_json(**context):
    uri = context['uri']
    filename = context['filename']
    results = requests.get(uri)
    data = results.json()
    with open(filename, 'w') as outfile:
        json.dump(data, outfile)

get_cdc_mortality_weekly = PythonOperator(
    task_id="dl_cdc_death_wk_json",
    python_callable=_api_death_all_data_json,
    op_kwargs={
        "uri": "https://data.cdc.gov/resource/muzy-jte6.json",
        "filename": '/tmp/cdc_api_testing/{{ execution_date.year }}{{ execution_date.month }}' \
                    '{{ execution_date.day }}{{ execution_date.hour }}.cdc_weekly_mortality.json'
    },
    provide_context=True,
    dag=dag,
)


def _json_to_csv(**context):
    json_input = context['filename']
    output = context['output_name']
    df = pandas.read_json(json_input)
    df.to_csv(output)

json_to_csv = PythonOperator(
    task_id="json_to_csv",
    python_callable=_json_to_csv,
    op_kwargs={
        "filename": '/tmp/cdc_api_testing/{{ execution_date.year }}{{ execution_date.month }}' \
                    '{{ execution_date.day }}{{ execution_date.hour }}' \
                     '.cdc_weekly_mortality.json',
        "output_name": "/tmp/cdc_api_testing/death_weekly_by_state.csv"
    },
    provide_context=True,
    dag=dag
)


get_cdc_mortality_weekly >> json_to_csv