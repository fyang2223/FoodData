import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from datetime import datetime, timedelta, date

from requestURL import pingLink
from visualisation import makegraph

AIRHOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
URL = "https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_csv_{{ ds }}.zip"
ZIPPED_FILENAME = "FoodData_{{ ds }}.zip"
CSVFOLDER = "FoodData_Central_csv_{{ ds }}"

with DAG(
    "USDA_ETL_DAG",
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
        bash_command=f"curl -o {AIRHOME}/data/{ZIPPED_FILENAME} {URL}"
    )

    t3 = BashOperator(
        task_id='Unzip',
        bash_command=f"unzip -o {AIRHOME}/data/{ZIPPED_FILENAME} -d {AIRHOME}/data"
    )

    t4 = SSHOperator(
        task_id='Load_into_HDFS',
        ssh_conn_id="hadoop_default",
        command=f"/load_data.sh {CSVFOLDER} "
    )

    t5 = BashOperator(
        task_id="Spark_job",
        bash_command=f"spark-submit {AIRHOME}/dags/spark_job.py {AIRHOME} {CSVFOLDER}"
        #application_args=[f"{AIRHOME}", f"{CSVFOLDER}"]
    )

    t6 = PythonOperator(
        task_id='Make_Graph',
        python_callable=makegraph, #Successful if 200 response code
        op_kwargs=dict(
            csv_loc=f"{AIRHOME}/data/output.csv" 
        )
    )

    t1.set_downstream(t2)
    t2.set_downstream(t3)
    t3.set_downstream(t4)
    t4.set_downstream(t5)
    t5.set_downstream(t6)
    