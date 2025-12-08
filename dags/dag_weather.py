from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path to find pipelines module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from airflow import DAG
from airflow.decorators import task, task_group
from pipelines.weather import pipe_weather
from pipelines.powerbi import utils


@task
def retrieve_history_weather_data():
    return pipe_weather.fetch_history_weather_data("Joinville")


@task
def format_history_weather_data(history_data):
    return pipe_weather.format_history_weather_data(history_data)


@task
def delete_history_weather_data():
    pipe_weather.delete_history_weather_data()


@task
def insert_history_weather_data_into_database(formatted_data):
    pipe_weather.insert_history_weather_data_into_database(formatted_data)


@task_group(group_id="process_historical_weather_data")
def process_historical_weather_data():
    history_data = retrieve_history_weather_data()
    formatted_data = format_history_weather_data(history_data)
    delete_history_weather_data()
    insert_history_weather_data_into_database(formatted_data)


@task
def retrieve_forecast_weather_data():
    return pipe_weather.fetch_forecast_weather_data("Joinville")


@task
def format_forecast_weather_data(forecast_data):
    return pipe_weather.format_forecast_weather_data(forecast_data)


@task
def delete_forecast_weather_data():
    pipe_weather.delete_forecast_weather_data()


@task
def insert_forecast_weather_data_into_database(formatted_data):
    pipe_weather.insert_forecast_weather_data_into_database(formatted_data)


@task_group(group_id="process_forecast_weather_data")
def process_forecast_weather_data():
    forecast_data = retrieve_forecast_weather_data()
    formatted_data = format_forecast_weather_data(forecast_data)
    delete_forecast_weather_data()
    insert_forecast_weather_data_into_database(formatted_data)


# @task
# def refresh_powerbi_dataset(dataset_id: str, group_id: str) -> None:
#     utils.refresh_powerbi_dataset(dataset_id, group_id)


with DAG(
    "weather_report",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple weather report DAG",
    schedule="0 3 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["weather"],
) as dag:
    hist = process_historical_weather_data()
    forecast = process_forecast_weather_data()
    refresh = refresh_powerbi_dataset(
        dataset_id="f857d834-69fc-4a0e-ae0f-9754ca2b9fd1",
        group_id="4573631b-396d-407b-ad18-159686c837de",
    )

    hist >> forecast >> refresh
