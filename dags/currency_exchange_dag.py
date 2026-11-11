
from datetime import datetime, timezone

import requests
from pymongo import MongoClient

from airflow import DAG
from airflow.models import Variable

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator


# -----------------------------------------
# TASK 1: EXTRACT CURRENCY EXCHANGE RATES
# -----------------------------------------

def extract_currency_rates(**context):

    url = "https://api.frankfurter.dev/v2/rates"

    params = {
        "base": "EUR",
        "quotes": "USD,GBP,JPY"
    }

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    raw_data = response.json()

    if not raw_data:
        raise ValueError("No currency rates returned by API")

    # Push data using a named XCom key
    context["ti"].xcom_push(
        key="raw_currency_rates",
        value=raw_data
    )

    print("Extracted currency rates:", raw_data)


# -----------------------------------------
# TASK 2: TRANSFORM EXCHANGE RATE DATA
# -----------------------------------------

def transform_currency_rates(**context):

    # Pull raw data using the named XCom key
    raw_data = context["ti"].xcom_pull(
        task_ids="extract_currency_rates",
        key="raw_currency_rates"
    )

    if not raw_data:
        raise ValueError("No raw data received from XCom")

    retrieved_at = datetime.now(
        timezone.utc
    ).isoformat(timespec="seconds")

    transformed_data = []

    for item in raw_data:

        record = {
            "base_currency": item["base"],
            "target_currency": item["quote"],
            "exchange_rate": float(item["rate"]),
            "rate_date": item["date"],
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at
        }

        transformed_data.append(record)

    # Push transformed data using another named XCom key
    context["ti"].xcom_push(
        key="transformed_currency_rates",
        value=transformed_data
    )

    print("Transformed data:", transformed_data)


# -----------------------------------------
# TASK 3: LOAD DATA INTO MONGODB
# -----------------------------------------

def load_to_mongodb(**context):

    # Pull transformed data using named XCom
    transformed_data = context["ti"].xcom_pull(
        task_ids="transform_currency_rates",
        key="transformed_currency_rates"
    )

    if not transformed_data:
        raise ValueError("No transformed data received")

    mongo_uri = Variable.get("MONGO_URI")

    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5000
    )

    try:
        client.admin.command("ping")

        db = client["currency_db"]

        collection = db["exchange_rates"]

        # Prevent duplicate records when the DAG runs again
        collection.create_index(
            [
                ("base_currency", 1),
                ("target_currency", 1),
                ("rate_date", 1)
            ],
            unique=True
        )

        for record in transformed_data:

            collection.update_one(
                {
                    "base_currency": record["base_currency"],
                    "target_currency": record["target_currency"],
                    "rate_date": record["rate_date"]
                },
                {"$set": record},
                upsert=True
            )

        print(
            f"Successfully loaded {len(transformed_data)} "
            "records into MongoDB"
        )

    finally:
        client.close()


# -----------------------------------------
# AIRFLOW DAG CONFIGURATION
# -----------------------------------------

with DAG(
    dag_id="currency_exchange_etl",
    description="Daily currency exchange ETL pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["currency", "mongodb", "etl"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_currency_rates
    )

    transform_task = PythonOperator(
        task_id="transform_currency_rates",
        python_callable=transform_currency_rates
    )

    load_task = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task
