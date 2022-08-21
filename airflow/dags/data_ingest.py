import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date

from requestURL import pingLink

AIRHOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
URL = "https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_csv_{{ ds }}.zip"
ZIPPED_FILENAME = "FoodData_{{ ds }}.zip"
CSVFOLDER = "FoodData_Central_csv_{{ ds }}"

with DAG(
    "URL_grab_and_ingest",
    start_date=datetime(2022, 4, 25),
    end_date=datetime(2022, 4, 30),
    schedule_interval=timedelta(days=1)

) as ingest_all:
    t1 = PythonOperator(
        task_id='pingURL',
        python_callable=pingLink, #Successful if 200 response code
        op_kwargs=dict(
            URL=URL 
        )
    )

    t2 = BashOperator(
        task_id='Download',
        bash_command=f"curl -o {AIRHOME}/{ZIPPED_FILENAME} {URL}"
    )

    t3 = BashOperator(
        task_id='Unzip',
        bash_command=f"unzip {AIRHOME}/{ZIPPED_FILENAME} -d {AIRHOME}"
    )

    t4 = BashOperator(
        task_id='new',
        bash_command=f'head {AIRHOME}/{CSVFOLDER}/acquisition_samples.csv'
    )

    t1.set_downstream(t2)
    t2.set_downstream(t3)
    t3.set_downstream(t4)



