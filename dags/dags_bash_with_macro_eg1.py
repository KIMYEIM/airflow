import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["lesson"],
) as dag:
  # START_DATE: 전월 말일, END_DATE: 1일 전
  bash_t1 = BashOperator(
    task_id="bash_t1",
    env = {
      'START_DATE': '{{ date_interval_start.in_timezone("Asia/Seoul") | ds}}',
      'END_DATE': '{{( date_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds }}',
    },
    bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
  )
  
  bash_t1
  