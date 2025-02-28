from airflow.models.dag import DAG
from airflow.decorators import task
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
with DAG(
  dag_id="dags_bash_with_variable",
  schedule="0 8 1 * *",
  start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
  catchup=False,
  tags=["lesson", "bash", "variable"],
) as dag:
  
  var_value = Variable.get("sample_key")

  bash_var_1 = BashOperator(
    task_id="bash_var_1",
    bash_command=f"echo variable:{var_value}"
  )

  bash_var_2 = BashOperator(
    task_id="bash_var_2",
    bash_command="echo variable:{{var.value.sample_key}}"
  )
  