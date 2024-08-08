from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    BranchPythonOperator,
    is_venv_installed,
)

with DAG(
        'pyspark_movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='pyspark_movie',
    schedule="10 2 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 3),
    catchup=True,
    tags=['pyspark','movie'],
) as dag:

    def fun_re_partition (ds_nodash):
        from spark_airflow.repartition import re_partition
        re_partition(ds_nodash)
        
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    re_partition = PythonVirtualenvOperator(
        task_id = "re_partition",
        python_callable = fun_re_partition,
        REQUIREMENTS = ["git+https://github.com/baechu805/spark_airflow.git@air"],
        system_site_packages = False,
        op_orgs=["{{ds_nodash}}"]
    )

    join_df = BashOperator(
        task_id="join.df",
        bash_command="""
        $SPARK_HOME/bin/spark-submit /home/sujin/code/pyspark_airflow/pyspark/sa.py "JOIN_TASK_APP" {{ds_nodash}}
        """
    )
    agg_df = BashOperator(
        task_id = 'agg_df',
        #python_callable = extract,
        #op_kwargs = { 'parq_path' : "{{var.value.TP_PATH}}/extract_path" },
        #system_site_packages = False,
        #requirements = REQUIREMENTS,
        #trigger_rule = "all_success",
        bash_command='''
        '''
    )
start >> re_partition >> join_df >> agg_df >> end
