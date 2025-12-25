from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os
import random

MODEL_VERSION = os.getenv("MODEL_VERSION", "v1.0.0")
METRIC_THRESHOLD = 0.8
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL", "mlops-team@example.com")

def train_model(**context):
    """
    Обучение модели 
    """
    print("Модель обучена")
    return "model_artifact_path"


def evaluate_model(**context):
    """
    Оценка модели
    """
    accuracy = round(random.uniform(0.7, 0.9), 3)
    print(f"Accuracy модели: {accuracy}")
    return accuracy


def decide_deploy(**context):
    """
    Деплоить или нет
    """
    accuracy = context["ti"].xcom_pull(task_ids="evaluate_model")

    if accuracy >= METRIC_THRESHOLD:
        print("Метрика удовлетворительная - деплой разрешён")
        return "deploy_model"
    else:
        print("Метрика ниже порога - деплой отменён")
        return "skip_deploy"


def deploy_model(**context):
    """
    Деплой модели
    """
    print(f"Модель версии {MODEL_VERSION} выведена в продакшен")


with DAG(
    dag_id="ml_retrain_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="ML pipeline with deploy and email notification",
    tags=["ml", "retraining", "production"],
) as dag:

    train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    evaluate = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
    )

    branch = BranchPythonOperator(
        task_id="decide_deploy",
        python_callable=decide_deploy,
    )

    deploy = PythonOperator(
        task_id="deploy_model",
        python_callable=deploy_model,
    )

    skip = EmptyOperator(
        task_id="skip_deploy"
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to=NOTIFY_EMAIL,
        subject="Новая модель выведена в продакшен",
        html_content=f"""
        <h3>Деплой модели выполнен успешно</h3>
        <p><b>Версия модели:</b> {MODEL_VERSION}</p>
        <p>Модель прошла валидацию и доступна в продакшене.</p>
        """,
    )

    train >> evaluate >> branch
    branch >> deploy >> notify_success
    branch >> skip
