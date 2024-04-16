from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 
import airflow.settings
from airflow.models import DagModel
import time

from datetime import datetime, timedelta, date 
from textwrap import dedent
import sys

default_args = { 
    'owner': 'ZheSun', 
    'depends_on_past': False, 
} 

with DAG(
    dag_id="5gHomeScore",
    default_args=default_args,
    description="get 5g Home Score",
    schedule_interval="00 21 * * *",
    start_date=days_ago(1),
    tags=["5gHomeScore"],
    catchup=False,
    max_active_runs=1,
) as dag:
    task_5gHomeClass = BashOperator( 
                        task_id="5gHomeClass", 
                        bash_command = f"/usr/apps/vmas/script/ZS/5gHomeScore/5gHomeClass.sh ", 
                        env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
                        dag=dag, 
                            ) 

    task_omaAgg = BashOperator( 
                    task_id="omaAgg", 
                    bash_command = f"/usr/apps/vmas/script/ZS/5gHomeScore/omaAgg.sh ", 
                    env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
                    dag=dag, 
                        ) 
    task_pg2hdfs = BashOperator( 
                task_id="pg2hdfs", 
                bash_command = f"/usr/apps/vmas/script/ZS/5gHomeScore/pg2hdfs.sh ", 
                env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
                dag=dag, 
                    ) 
    task_join_all = BashOperator( 
                task_id="join_all", 
                bash_command = f"/usr/apps/vmas/script/ZS/5gHomeScore/join_all.sh ", 
                env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
                dag=dag, 
                    ) 
    task_featureToScore = BashOperator( 
                task_id="featureToScore", 
                bash_command = f"/usr/apps/vmas/script/ZS/5gHomeScore/featureToScore.sh ", 
                env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
                dag=dag, 
                    ) 
    [ task_5gHomeClass ,task_omaAgg,task_pg2hdfs ] >> task_join_all >> task_featureToScore