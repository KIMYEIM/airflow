from airflow.models.dag import DAG
from airflow.decorators import task
import pendulum

with DAG(
  dag_id="dags_python_with_xcom_eg2",
  schedule="30 6 * * *",
  start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
  catchup=False,
  tags=["lesson", "python"],
) as dag:
  
  @task(task_id='xcom_push_by_return')
  def xcom_push_by_return():
    return "Success"

  @task(task_id='xcom_pull_by_return1')
  def xcom_pull_by_return1(**kwargs):
    ti = kwargs.get("ti")
    value1 = ti.xcom_pull(task_ids="xcom_push_by_return")
    print('xcom_pull로 찾은 값:', value1)

  @task(task_id='xcom_pull_by_return2')
  def xcom_pull_by_return2(status, **kwargs):
    print('함수 입력값으로 찾은 값:', status)

  python_xcom_push_by_return = xcom_push_by_return()
  xcom_pull_by_return2(python_xcom_push_by_return)
  python_xcom_push_by_return >> xcom_pull_by_return1()