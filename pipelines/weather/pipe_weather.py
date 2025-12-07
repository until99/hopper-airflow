from datetime import datetime, timedelta
import requests
import dotenv
import os
import pandas as pd
from sqlalchemy import (
    create_engine,
    insert,
    delete,
    Column,
    Integer,
    String,
    DateTime,
    Float,
    Date,
)
from sqlalchemy.orm import sessionmaker, declarative_base

dotenv.load_dotenv()

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

if db_port and db_port.lower() != 'none':
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
else:
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"

engine = create_engine(db_url)

Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


class Forecast(Base):
    __tablename__ = "forecast"
    id = Column(Integer, primary_key=True, autoincrement=True)
    is_forecast = Column(Integer)
    name = Column(String)
    region = Column(String)
    country = Column(String)
    lat = Column(String)
    lon = Column(String)
    time_epoch = Column(Integer)
    date = Column(Date)
    time = Column(DateTime)
    temp_c = Column(Float)
    temp_f = Column(Float)
    is_day = Column(Integer)
    condition = Column(String)
    condition_icon = Column(String)
    condition_code = Column(Integer)
    wind_mph = Column(Float)
    wind_kph = Column(Float)
    wind_degree = Column(Float)
    wind_dir = Column(String)
    pressure_mb = Column(Float)
    pressure_in = Column(Float)
    precip_mm = Column(Float)
    precip_in = Column(Float)
    snow_cm = Column(Float)
    humidity = Column(Float)
    cloud = Column(Float)
    feelslike_c = Column(Float)
    feelslike_f = Column(Float)
    windchill_c = Column(Float)
    windchill_f = Column(Float)
    heatindex_c = Column(Float)
    heatindex_f = Column(Float)
    dewpoint_c = Column(Float)
    dewpoint_f = Column(Float)
    will_it_rain = Column(Integer)
    chance_of_rain = Column(Integer)
    will_it_snow = Column(Integer)
    chance_of_snow = Column(Integer)
    vis_km = Column(Float)
    vis_miles = Column(Float)
    gust_mph = Column(Float)
    gust_kph = Column(Float)
    uv = Column(Float)


def yesterday() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def tomorrow() -> str:
    return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


def today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def fetch_forecast_weather_data(city: str) -> dict:
    response = requests.get(
        f"{os.getenv('WEATHER_API_URL')}/forecast.json?key={os.getenv('WEATHER_API_KEY')}&q={city}&days=2&aqi=no&alerts=no",
        verify=False,
    )
    return response.json()


def fetch_history_weather_data(city: str, date: str = yesterday()) -> dict:
    response = requests.get(
        f"{os.getenv('WEATHER_API_URL')}/history.json?key={os.getenv('WEATHER_API_KEY')}&q={city}&dt={date}&aqi=no&alerts=no",
        verify=False,
    )

    return response.json()


def format_history_weather_data(data: dict) -> pd.DataFrame:
    forecast_records = []

    for hour in data["forecast"]["forecastday"][0]["hour"]:
        forecast_records.append(
            {
                "is_forecast": 0,
                "name": data["location"]["name"],
                "region": data["location"]["region"],
                "country": data["location"]["country"],
                "lat": data["location"]["lat"],
                "lon": data["location"]["lon"],
                "time_epoch": hour["time_epoch"],
                "date": hour["time"].split(" ")[0],
                "time": hour["time"],
                "temp_c": hour["temp_c"],
                "temp_f": hour["temp_f"],
                "is_day": hour["is_day"],
                "condition": hour["condition"]["text"],
                "condition_icon": hour["condition"]["icon"],
                "condition_code": hour["condition"]["code"],
                "wind_mph": hour["wind_mph"],
                "wind_kph": hour["wind_kph"],
                "wind_degree": hour["wind_degree"],
                "wind_dir": hour["wind_dir"],
                "pressure_mb": hour["pressure_mb"],
                "pressure_in": hour["pressure_in"],
                "precip_mm": hour["precip_mm"],
                "precip_in": hour["precip_in"],
                "snow_cm": hour["snow_cm"],
                "humidity": hour["humidity"],
                "cloud": hour["cloud"],
                "feelslike_c": hour["feelslike_c"],
                "feelslike_f": hour["feelslike_f"],
                "windchill_c": hour["windchill_c"],
                "windchill_f": hour["windchill_f"],
                "heatindex_c": hour["heatindex_c"],
                "heatindex_f": hour["heatindex_f"],
                "dewpoint_c": hour["dewpoint_c"],
                "dewpoint_f": hour["dewpoint_f"],
                "will_it_rain": hour["will_it_rain"],
                "chance_of_rain": hour["chance_of_rain"],
                "will_it_snow": hour["will_it_snow"],
                "chance_of_snow": hour["chance_of_snow"],
                "vis_km": hour["vis_km"],
                "vis_miles": hour["vis_miles"],
                "gust_mph": hour["gust_mph"],
                "gust_kph": hour["gust_kph"],
                "uv": hour["uv"],
            }
        )

    forecast_df = pd.DataFrame(forecast_records)

    return forecast_df


def format_forecast_weather_data(data: dict) -> pd.DataFrame:
    forecast_records = []
    for hour in data["forecast"]["forecastday"][1]["hour"]:
        forecast_records.append(
            {
                "is_forecast": 1,
                "name": data["location"]["name"],
                "region": data["location"]["region"],
                "country": data["location"]["country"],
                "lat": data["location"]["lat"],
                "lon": data["location"]["lon"],
                "time_epoch": hour["time_epoch"],
                "date": hour["time"].split(" ")[0],
                "time": hour["time"],
                "temp_c": hour["temp_c"],
                "temp_f": hour["temp_f"],
                "is_day": hour["is_day"],
                "condition": hour["condition"]["text"],
                "condition_icon": hour["condition"]["icon"],
                "condition_code": hour["condition"]["code"],
                "wind_mph": hour["wind_mph"],
                "wind_kph": hour["wind_kph"],
                "wind_degree": hour["wind_degree"],
                "wind_dir": hour["wind_dir"],
                "pressure_mb": hour["pressure_mb"],
                "pressure_in": hour["pressure_in"],
                "precip_mm": hour["precip_mm"],
                "precip_in": hour["precip_in"],
                "snow_cm": hour["snow_cm"],
                "humidity": hour["humidity"],
                "cloud": hour["cloud"],
                "feelslike_c": hour["feelslike_c"],
                "feelslike_f": hour["feelslike_f"],
                "windchill_c": hour["windchill_c"],
                "windchill_f": hour["windchill_f"],
                "heatindex_c": hour["heatindex_c"],
                "heatindex_f": hour["heatindex_f"],
                "dewpoint_c": hour["dewpoint_c"],
                "dewpoint_f": hour["dewpoint_f"],
                "will_it_rain": hour["will_it_rain"],
                "chance_of_rain": hour["chance_of_rain"],
                "will_it_snow": hour["will_it_snow"],
                "chance_of_snow": hour["chance_of_snow"],
                "vis_km": hour["vis_km"],
                "vis_miles": hour["vis_miles"],
                "gust_mph": hour["gust_mph"],
                "gust_kph": hour["gust_kph"],
                "uv": hour["uv"],
            }
        )

    forecast_df = pd.DataFrame(forecast_records)

    return forecast_df


def delete_history_weather_data(date: str = yesterday()) -> None:
    stmt = delete(Forecast).where(
        Forecast.date == date,
        Forecast.is_forecast == 0,
    )

    try:
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    except Exception as e:
        print(e)


def delete_forecast_weather_data(
    date: str = tomorrow(),
) -> None:
    stmt = delete(Forecast).where(
        Forecast.date == date,
        Forecast.is_forecast == 1,
    )

    try:
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    except Exception as e:
        print(e)


def insert_history_weather_data_into_database(data: pd.DataFrame) -> None:
    Base.metadata.create_all(engine)

    stmt = insert(Forecast).values(data.to_dict(orient="records"))

    try:
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    except Exception as e:
        print(e)


def insert_forecast_weather_data_into_database(data: pd.DataFrame) -> None:
    Base.metadata.create_all(engine)

    stmt = insert(Forecast).values(data.to_dict(orient="records"))

    try:
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    except Exception as e:
        print(e)


def populate_historical_weather_data_by_date_range(
    city: str, start_date: str, end_date: str
) -> None:
    """
    Popula a base de dados com informações históricas de clima para um range de datas.

    Args:
        city: Nome da cidade para buscar os dados
        start_date: Data inicial no formato 'YYYY-MM-DD'
        end_date: Data final no formato 'YYYY-MM-DD'

    Exemplo:
        populate_historical_weather_data_by_date_range('Joinville', '2025-10-01', '2025-10-15')
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current_date = start
    total_days = (end - start).days + 1
    processed_days = 0

    print(
        f"Iniciando carga histórica de {start_date} até {end_date} ({total_days} dias)"
    )

    while current_date <= end:
        date_str = current_date.strftime("%Y-%m-%d")

        try:
            print(f"Processando data: {date_str}")

            history_data = fetch_history_weather_data(city, date_str)

            formatted_data = format_history_weather_data(history_data)

            delete_history_weather_data(date_str)

            insert_history_weather_data_into_database(formatted_data)

            processed_days += 1
            print(
                f"Data {date_str} processada com sucesso ({processed_days}/{total_days})"
            )

        except Exception as e:
            print(f"Erro ao processar data {date_str}: {e}")

        current_date += timedelta(days=1)

    print(
        f"\nCarga histórica concluída! {processed_days}/{total_days} dias processados."
    )


if __name__ == "__main__":
    # forecast_data = fetch_forecast_weather_data("Joinville")
    # forecast_data = format_forecast_weather_data(forecast_data)
    # delete_forecast_weather_data()
    # insert_forecast_weather_data_into_database(forecast_data)

    # start_date = "2025-10-30"
    # end_date = "2025-10-30"
    # date_list = (
    #     pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    # )

    # for date in date_list:
    #     print(f"Processando data: {date}")
    #     weather_data = fetch_history_weather_data("Joinville", date)
    #     weather_data = format_history_weather_data(weather_data)

    #     print(f"{len(weather_data)} registros formatados")

    #     # delete_history_weather_data(date)
    #     insert_history_weather_data_into_database(weather_data)
    #     print("Dados inseridos no banco com sucesso!")
