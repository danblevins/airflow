from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import datetime
import pendulum
from datetime import date

RESULT_KEY_NAME = "result_"
CURRENT_DATE = str(date.today())

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2022, 3, 1, tz="America/Los_Angeles"),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=1),
}


def save_current_date(task_instance, **kwargs):
    key_name = CURRENT_DATE
    task_instance.xcom_push(key=key_name, value=CURRENT_DATE)

    return True


def get_word_of_the_day(task_instance, **kwargs):
    import requests
    import json
    import os
    from urllib.request import urlopen

    api_key = os.environ.get("WORDNIK_API_KEY")
    key_name = CURRENT_DATE

    current_date_xcom = task_instance.xcom_pull(
        key=key_name, task_ids=["save_current_date"]
    )[0]

    url = "http://api.wordnik.com:80/v4/words.json/wordOfTheDay?date={}&api_key={}".format(
        current_date_xcom, api_key
    )
    response = urlopen(url)
    word = json.load(response)

    result = {
        "word_id": word["_id"],
        "word": word["word"],
        "definition": word["definitions"][0]["text"],
    }

    key_name = RESULT_KEY_NAME + current_date_xcom
    task_instance.xcom_push(key=key_name, value=result)

    return True


def save_to_mongodb(task_instance, **kwargs):
    import os
    import certifi
    import pymongo

    mongodb_username = os.environ.get("mongodb_username")
    mongodb_password = os.environ.get("mongodb_password")
    mongodb_database = os.environ.get("mongodb_database")
    mongodb_authentication = "mongodb+srv://{}:{}@dan.nspqx.mongodb.net/{}?retryWrites=true&w=majority".format(mongodb_username, mongodb_password, mongodb_database)

    connect = pymongo.MongoClient(mongodb_authentication, tlsCAFile=certifi.where())
    db = connect.wordnik
    collection = db.word_of_the_day

    key_name = RESULT_KEY_NAME + CURRENT_DATE
    data_dict = task_instance.xcom_pull(key=key_name, task_ids=["get_word_of_the_day"])[0]
    collection.insert_one(data_dict)

    return True


with DAG(
    "daily_word_of_the_day_to_mongodb",
    default_args=default_args,
    description="Save Daily Word of the Day to MongoDB",
    schedule_interval=datetime.timedelta(days=1),
    catchup=False,
) as dag:

    save_current_date = PythonOperator(
        task_id="save_current_date",
        python_callable=save_current_date,
        dag=dag,
        do_xcom_push=False,
    )

    get_word_of_the_day = PythonOperator(
        task_id="get_word_of_the_day",
        python_callable=get_word_of_the_day,
        dag=dag,
        do_xcom_push=False,
    )

    save_to_mongodb = PythonOperator(
        task_id="save_to_mongodb",
        python_callable=save_to_mongodb,
        dag=dag,
        do_xcom_push=False,
    )

    save_current_date >> get_word_of_the_day >> save_to_mongodb
