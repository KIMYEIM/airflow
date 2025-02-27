import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
  dag_id="dags_bash_with_xcom",
  schedule="10 0 * * *",
  start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
  catchup=False,
  tags=["lesson", "bash"],
) as dag:
  
  bash_push = BashOperator(
    task_id="bash_push",
    bash_command="echo START &&"
    "echo XCOM_PUSHED &&"
    "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message')}} &&"
    "echo COMPLETE"
  )
  
  bash_pull = BashOperator(
    task_id="bash_pull",
    env={ 'pushed_value':"{{ ti.xcom_pull(key='bash_pushed') }}",
          'return_value':"{{ ti.xcom_pull(task_ids='bash_push') }}"},
    bash_command="echo $pushed_value && echo $return_value",
    do_xcom_push=False
  )
  
  bash_push >> bash_pull