from airflow.models.dag import DAG
from airflow.decorators import task
import pendulum

with DAG(
  dag_id="dags_python_with_xcom_eg1",
  schedule="30 6 * * *",
  start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
  catchup=False,
  tags=["lesson", "python"],
) as dag:

  @task(task_id="task_xcom_push1")
  def xcom_push1(**kwargs):
    ti = kwargs.get("ti")
    ti.xcom_push(key="result1", value="value_1")
    ti.xcom_push(key="result2", value=[1, 2, 3])

  @task(task_id="task_xcom_push2")
  def xcom_push2(**kwargs):
    ti = kwargs.get("ti")
    ti.xcom_push(key="result1", value="value_2")
    ti.xcom_push(key="result2", value=[4, 5, 6])

  @task(task_id="task_xcom_pull")
  def xcom_pull(**kwargs):
    ti = kwargs.get("ti")
    value1 = ti.xcom_pull(key="result1")
    value2 = ti.xcom_pull(task_ids="task_xcom_push1", key="result2")
    print(value1)
    print(value2)

  xcom_push1() >> xcom_push2() >> xcom_pull()
