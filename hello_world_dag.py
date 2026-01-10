from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def _print_hello():
    print("Hello World")


with DAG(
    'hello_world',
    description='Simple Hello World DAG',
    schedule='@once',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    print_hello = PythonOperator(
        task_id='print_hello',
        python_callable=_print_hello,
    )


if __name__ == '__main__':
    dag.test()
