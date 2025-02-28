from airflow.models.dag import DAG
from airflow.decorators import task
import pendulum
from airflow.operators.bash import BashOperator
with DAG(
  dag_id="dags_bash_python_with_xcom",
  schedule="30 6 * * *",
  start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
  catchup=False,
  tags=["lesson", "python", "bash"],
) as dag:

  @task(task_id="python_push")
  def python_push(**kwargs):
    result_dict = {'status': 'Good', 'data': [1, 2, 3], 'options_cnt': 100}
    return result_dict
  
  bash_pull = BashOperator(
    task_id="bash_pull",
    env={ 
      'STATUS':"{{ ti.xcom_pull(task_ids='python_push')['status'] }}",
      'DATA':"{{ ti.xcom_pull(task_ids='python_push')['data'] }}",
      'OPTIONS_CNT':"{{ ti.xcom_pull(task_ids='python_push')['options_cnt'] }}"
    },
    bash_command="echo $STATUS && echo $DATA && echo $OPTIONS_CNT",
    do_xcom_push=False
  )

  python_push() >> bash_pull

  bash_push = BashOperator(
    task_id="bash_push",
    bash_command="echo PUSH_START "
    "{{ti.xcom_push(key='bash_pushed', value=200)}} ;"
    "echo PUSH_COMPLETE"
  )
  
  @task(task_id="python_pull")
  def python_pull(**kwargs):
    ti = kwargs.get("ti")
    status_value = ti.xcom_pull(key="bash_pushed")
    return_value = ti.xcom_pull(task_ids="bash_push")
    print(str(status_value))
    print(return_value)
    
  bash_push >> python_pull()    
    