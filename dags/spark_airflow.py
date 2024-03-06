import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import sys
import os

# Add the project root to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from jobs.spotify_data import generate_spotify_data

# from jobs.counter import count_words

dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "owner": "James Smart",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@10pm"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

spotify_data_generator = PythonOperator(
    task_id='spotify_data',
    python_callable=generate_spotify_data,
    op_kwargs={'genres': ['Gospel', 'R&B', 'pop', 'rock', 'hip-hop', 'jazz', 'country', 'electronic', 'classical',
                          'reggae', 'blues', 'folk', 'indie', 'metal', 'punk', 'soul', 'disco', 'funk', 'ambient',
                          'techno']},
    dag=dag,
)

sparks_analytic_job = SparkSubmitOperator(
    task_id="sparks_analytic_job",
    conn_id="spark-conn",
    application="jobs/counter.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> spotify_data_generator >> sparks_analytic_job >> end
