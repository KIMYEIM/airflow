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
    "echo XCOM_PUSH &&"
    "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message')}} &&"
    "echo COMPLETE"
  )
  
  bash_pull = BashOperator(
    task_id="bash_pull",
    env={
      'PUSHED_VALUE': "{{ ti.xcom_pull(task_ids='bash_push')}}",
      'RETURN_VALUE': "{{ ti.xcom_pull(task_ids='bash_push')}}"
    },
    bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE",
    do_xcom_push=False
  )
  
  bash_push >> bash_pull