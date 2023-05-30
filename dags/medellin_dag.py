from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow import DAG

from fincaRaiz_etl import extract_data, clean_data, db_raw

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 25, 21, 25, 0, 0),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False
}


with DAG(
    dag_id="medellin_extract",
    default_args=default_args,
    description="finca raiz extract",
    # schedule_interval=timedelta(minutes=3),
) as dag:
    create_db_raw = PythonOperator(
        task_id="create_db_raw",
        python_callable=db_raw,
        op_kwargs={"city": "medellin"},
    )

    clean_data_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        op_kwargs={"city": "medellin"},
    )

    for i in range(1, 278): # total pages
        extract_task = PythonOperator(
            task_id="extract_{}".format(i),
            python_callable=extract_data,
            op_kwargs={
                "number_page": i,
                "city": "medellin",
                "state": "antioquia",
            },
        )

        create_db_raw >> extract_task >> clean_data_task
