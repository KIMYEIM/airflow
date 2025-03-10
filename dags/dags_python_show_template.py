import pendulum
from airflow.models.dag import DAG
from airflow.decorators import task

with DAG(
    dag_id="dags_python_show_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 2, 20, tz="Asia/Seoul"),
    catchup=True,
    tags=["lesson"],
) as dag:
  
  @task(task_id="python_task")
  def show_templates(**kwargs):
    from pprint import pprint
    pprint(kwargs)

  show_templates()