from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator

def transaction():
    print("Transaction pipeline started at ", datetime.now())

def total_transactions():
    transactions = [120, 300, 450, 200]
    total = sum(transactions)
    with open("/data/transactions_report.txt", "w") as f:
        f.write("Daily Transaction Report\n")
        f.write("Number of transactions: {}\n".format(len(transactions)))
        f.write("Total amount: {}\n".format(total))

def send_email_report():
    with open("/data/transactions_report.txt", "r") as f:
        report_content = f.read()
    return report_content


with DAG (
    dag_id="Transaction_Pipeline",
    start_date=datetime(2026, 4, 7),
    schedule_interval=timedelta(minutes=10),
    catchup=False
) as dag:
    start_message = PythonOperator (
        task_id= "start_message",
        python_callable = transaction
    )
    process_transactions = PythonOperator (
        task_id= "process_transactions",
        python_callable = total_transactions
    )
    run_external_script = BashOperator (
        task_id = "run_external_script",
        bash_command="python /data/process_transactions.py"
    )
    # Reading the report before sending it in the email
    report_email = PythonOperator (
        task_id = "send_email_report",
        python_callable = send_email_report
    )
    send_email = EmailOperator (
        task_id = "send_email",
        to = 'elsayedmachinel@gmail.com',
        subject = 'Transaction Report',
        html_content="<pre>{{ task_instance.xcom_pull(task_ids='send_email_report') }}</pre>"
    )
    
start_message >> process_transactions >> run_external_script >> report_email >> send_email
