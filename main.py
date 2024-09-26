import pandas as pd
import requests
import psycopg2
import time
import threading
from psycopg2 import Error

# Настройки API
api_key = 'ваш_API_ключ'
city = 'Moscow,RU'
base_url = 'http://api.openweathermap.org/data/2.5/weather'

# Настройки базы данных
db_config = {
    'host': 'localhost',
    'database': 'weather_db',
    'user': 'your_username',
    'password': 'your_password'
}


def get_wind_direction(deg):
    directions = ['С', 'СВ', 'В', 'ЮВ', 'Ю', 'ЮЗ', 'З', 'СЗ']
    return directions[int((deg % 360) / 45) % 8]


def add_to_database(temperature, wind_speed, wind_direction, pressure, precipitation):
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO weather_data (temperature, wind_speed, wind_direction, pressure, precipitation)
            VALUES (%s, %s, %s, %s, %s)
        """, (temperature, wind_speed, wind_direction, pressure, precipitation))
        connection.commit()
        print("Данные добавлены в базу данных")
    except Error as e:
        print(f"Ошибка: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()


def export_to_excel():
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM weather_data ORDER BY created_at DESC LIMIT 10")
        data = cursor.fetchall()
        columns = [desc for desc in cursor.description]
        df = pd.DataFrame(data, columns=columns)
        df.to_excel('weather_data.xlsx', index=False)
        print("Данные экспортированы в файл weather_data.xlsx")
    except Error as e:
        print(f"Ошибка: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()


def start_export():
    thread = threading.Thread(target=export_to_excel)
    thread.start()


def main():
    params = {
        'q': city,
        'units': 'metric',
        'lang': 'ru',
        'APPID': api_key
    }

    while True:
        try:
            response = requests.get(base_url, params=params)
            data = response.json()
            temperature = data['main']['temp']
            wind_speed = data['wind']['speed']
            wind_direction = get_wind_direction(data['wind']['deg'])
            pressure = data['main']['pressure'] * 0.750062  # в мм рт. ст.
            precipitation = 'Нет осадков' if 'rain' not in data else data['rain']['1h']

            add_to_database(temperature, wind_speed, wind_direction, pressure, precipitation)
        except Exception as e:
            print(f"Exception: {e}")
        time.sleep(180)  # пауза на 3 минуты


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv == 'export':
        start_export()
    else:
        main()
