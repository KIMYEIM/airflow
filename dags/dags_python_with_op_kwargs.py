import pendulum
from airflow.models.dag import DAG
from common.common_func import regist2
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["lesson"],
) as dag:
  
  regist2_t1 = PythonOperator(
    task_id="regist2_t1",
    python_callable=regist2,
    op_args=["홍길동", "남자", "kr", "seoul"],
    op_kwargs={"email": "hong@gmail.com", "phone": "01012345678"},
  )
  
  regist2_t1