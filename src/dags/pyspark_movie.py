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
    end_date=datetime(2015, 1, 11),
    catchup=True,
    tags=['pyspark','movie'],
) as dag:

    def fun_re_partition (dt): # ds_nodash 인자 전달
        from spark_airflow.repartition import re_partition
        re_partition(dt)
        
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    re_partition = PythonVirtualenvOperator(
        task_id = "re_partition",
        python_callable = fun_re_partition,
        requirements = ["git+https://github.com/baechu805/spark_airflow.git@def"], #외부패키지 설치
        system_site_packages = False,
        op_args=["{{ds_nodash}}"] # 함수 인자 지정
    )

    join_df = BashOperator(
        task_id="join_df",
        # 실행 명령어 뒷 경로 실행
        bash_command="""
        $SPARK_HOME/bin/spark-submit /home/joo/code/spark_airflow/pyspark/movie_join_df.py {{ds_nodash}}
        """
# "JOIN_TASK_APP"와  {{ds_nodash}} 인자로 가짐
    )
    agg_df = BashOperator(
        task_id = 'agg_df',
        bash_command='''
        $SPARK_HOME/bin/spark-submit /home/joo/code/spark_airflow/pyspark/movie_sum_df.py {{ds_nodash}}
        '''
    )
start >> re_partition >> join_df >> agg_df >> end
