try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def onboard_customer_function_execute(**context):
    print("Executing the onboarding customer function")
    context['ti'].xcom_push(key='customer_key', value='1')


def account_creation_function_execute(**context):
    print("Creating customer account")
    customer_id = context.get("ti").xcom_pull(key='customer_key')
    data = [{"customer_id": customer_id}]
    df = pd.DataFrame(data=data)
    print(df.head())


with DAG(
        dag_id="onboard_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2020, 1, 1),
        },
        catchup=False) as f:
    onboard_customer_function_execute = PythonOperator(
        task_id="onboard_customer",
        python_callable=onboard_customer_function_execute,
        provide_context=True,
        op_kwargs={"customer_id": "1"}
    )

    account_creation_function_execute = PythonOperator(
        task_id="account_creation_function_execute",
        python_callable=account_creation_function_execute,
        provide_context=True,
    )

onboard_customer_function_execute >> account_creation_function_execute
