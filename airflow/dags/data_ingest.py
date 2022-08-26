import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

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
        bash_command=f"curl -o {AIRHOME}/data/{ZIPPED_FILENAME} {URL}"
    )

    t3 = BashOperator(
        task_id='Unzip',
        bash_command=f"unzip {AIRHOME}/data/{ZIPPED_FILENAME} -d {AIRHOME}/data"
    )

    t4 = BashOperator(
        task_id='head',
        # bash_command=f'$SPARK_HOME/sbin/start-master.sh --webui-port 8081'
        bash_command=f'echo hello'
    )

    # t5 = SparkSubmitOperator(
    #     task_id = "spark-job",
    #     application = f"{AIRHOME}/dags/spark-job.py",
    #     conn_id = "spark_default",
    #     application_args = [f"{AIRHOME}", f"{CSVFOLDER}"]
    # )

    t5 = BashOperator(
        task_id="spark-job",
        bash_command=f"spark-submit {AIRHOME}/dags/spark-job.py {AIRHOME} {CSVFOLDER}"
        #application_args=[f"{AIRHOME}", f"{CSVFOLDER}"]
    )

    t6 = SSHOperator(
        task_id="sshIntoHadoop",
        ssh_conn_id="hadoop_default",
        command="ls"


    )





    t6.set_downstream(t1)

    # t2.set_downstream(t3)
    # t1.set_downstream(t2)
    # t3.set_downstream(t4)
    # t4.set_downstream(t5)


