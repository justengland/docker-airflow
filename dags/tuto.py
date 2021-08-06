"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("tutorial",
          max_active_runs=3,
         start_date=datetime(2020, 2, 20, 13, 0),
         schedule_interval='@once',
         default_args=default_args)


def py_hi():
    print('what')


# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = PythonOperator(
    task_id='pyHi',
    python_callable=py_hi,
    dag=dag,
)

t2 = PythonOperator(
    task_id='pyHi2',
    python_callable=py_hi,
    dag=dag,
)
#
# templated_command = """
#     {% for i in range(5) %}
#         echo "{{ ds }}"
#         echo "{{ macros.ds_add(ds, 7)}}"
#         echo "{{ params.my_param }}"
#     {% endfor %}
# """
#
# t3 = BashOperator(
#     task_id="templated",
#     bash_command=templated_command,
#     params={"my_param": "Parameter I passed in"},
#     dag=dag,
# )

# t2.set_upstream(t1)
# t3.set_upstream(t1)

t1 >> t2
