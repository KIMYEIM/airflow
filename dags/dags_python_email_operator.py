from airflow.models.dag import DAG
from airflow.decorators import task
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
  dag_id="dags_python_email_operator",
  schedule="0 8 1 * *",
  start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
  catchup=False,
  tags=["lesson", "python", "email"],
) as dag:

  @task(task_id="something_task")
  def something_task(**kwargs):
    from random import choice
    return choice(['Success', 'Failed'])
  
  send_email = EmailOperator(
    task_id="send_email",
    to="kyaeim@naver.com",
    subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} something_task 처리결과',
    html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br> \
    {{ti.xcom_pull(task_ids="something_task")}} 했습니다 <br>',
  )
  
  something_task() >> send_email
