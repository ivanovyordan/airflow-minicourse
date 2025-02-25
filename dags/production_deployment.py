import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Function to simulate a condition for branching
def branch_decision(**kwargs):
    # Simulate a condition (e.g., checking a variable or a status flag)
    # For demonstration, we'll branch based on the current minute (even/odd)
    current_minute = datetime.now().minute

    if current_minute % 2 == 0:
        logging.info("Even minute detected. Proceeding with success path.")
        return "success_path"
    else:
        logging.info("Odd minute detected. Proceeding with failure path.")
        return "failure_path"

# Function for the success branch
def success_task(**kwargs):
    logging.info("Success branch executed successfully!")

    # Simulate further processing
    return "Success"

# Function for the failure branch
def failure_task(**kwargs):
    logging.info("Failure branch executed. Handling failure scenario.")

    # Simulate error recovery or alerting
    return "Failure Handled"

# Function to dynamically generate tasks
def dynamic_task(task_number, **kwargs):
    logging.info("Executing dynamic task number: %d", task_number)

    # Simulate processing that could be dynamically defined
    return f"Task {task_number} completed"

# Function to simulate Slack alert on failures
def slack_alert(**kwargs):
    # In a real scenario, you would call an API endpoint here
    logging.warning("Sending Slack alert: One or more tasks have failed!")

    # Simulated alert (replace with requests.post() to your Slack webhook)
    return "Slack alert sent"

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    'retries': 1,
}

with DAG(
    dag_id='production_deployment',
    default_args=default_args,
    description='A DAG demonstrating production best practices with branching and dynamic tasks',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task: Branching decision based on a condition.
    branching = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=branch_decision,
        provide_context=True
    )

    # Success path task
    success = PythonOperator(
        task_id='success_path',
        python_callable=success_task,
        provide_context=True
    )

    # Failure path task
    failure = PythonOperator(
        task_id='failure_path',
        python_callable=failure_task,
        provide_context=True
    )

    # Dummy task to join branch outcomes
    join = EmptyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Dynamic Task Generation: Create 3 dynamic tasks in a loop.
    dynamic_tasks = []
    for i in range(1, 4):
        task = PythonOperator(
            task_id=f'dynamic_task_{i}',
            python_callable=dynamic_task,
            op_kwargs={'task_number': i},
            provide_context=True
        )
        dynamic_tasks.append(task)

    # Task: Slack alert for failure scenarios.
    notify_failure = PythonOperator(
        task_id='notify_failure',
        python_callable=slack_alert,
        trigger_rule=TriggerRule.ONE_FAILED,
        provide_context=True
    )

    # Define the DAG flow:
    # 1. Branch decision runs first.
    # 2. Depending on the branch, either success or failure task runs.
    # 3. Both branches converge at 'join' task.
    # 4. After joining, dynamic tasks are executed in parallel.
    # 5. If any task fails, notify_failure runs.
    branching >> [success, failure] >> join
    join >> dynamic_tasks >> notify_failure

    # Homework: Research Airflow Sensors and implement one to monitor a data threshold or file arrival.
