from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from datetime import datetime, timedelta

def subdag_factory(parent_dag_name, child_dag_name, start_date, schedule_interval):
    subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        start_date=start_date,
        schedule_interval=schedule_interval,
        catchup=False
    )
    task1 = PythonOperator(
        task_id='task1',
        python_callable=lambda: print(f"{child_dag_name} - task1 executed"),
        dag=subdag
    )
    task2 = PythonOperator(
        task_id='task2',
        python_callable=lambda: print(f"{child_dag_name} - task2 executed"),
        dag=subdag
    )
    task1 >> task2
    return subdag

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'subdag_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

start = DummyOperator(task_id='start', dag=dag)
level1 = SubDagOperator(
    task_id='Level1',
    subdag=subdag_factory('subdag_pipeline', 'Level1', default_args['start_date'], '@daily'),
    dag=dag
)
level2 = SubDagOperator(
    task_id='Level2',
    subdag=subdag_factory('subdag_pipeline', 'Level2', default_args['start_date'], '@daily'),
    dag=dag
)
level3 = SubDagOperator(
    task_id='Level3',
    subdag=subdag_factory('subdag_pipeline', 'Level3', default_args['start_date'], '@daily'),
    dag=dag
)
level4 = SubDagOperator(
    task_id='Level4',
    subdag=subdag_factory('subdag_pipeline', 'Level4', default_args['start_date'], '@daily'),
    dag=dag
)
end = DummyOperator(task_id='end', dag=dag)

start >> level1 >> level2 >> level3 >> level4 >> end
