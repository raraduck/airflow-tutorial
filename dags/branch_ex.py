from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime

dag = DAG(
    dag_id="branch_example",
    schedule="@once",
    start_date=datetime(2024, 12, 1)
)

run_this_first = EmptyOperator(task_id="run_this_first", dag=dag)


@task.branch(task_id="branching")
def do_branching():
    return "branch_a"


branching = do_branching()

branch_a = EmptyOperator(task_id="branch_a", dag=dag)
follow_branch_a = EmptyOperator(task_id="follow_branch_a", dag=dag)

branch_false = EmptyOperator(task_id="branch_false", dag=dag)

join = EmptyOperator(task_id="join", dag=dag)

run_this_first >> branching
branching >> branch_a >> follow_branch_a >> join
branching >> branch_false >> join
