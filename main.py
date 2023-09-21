import datetime
import sqlite3

import pandas as pd
from memory_profiler import profile


CLIENT_PATH = "client.csv"
SERVER_PATH = "server.csv"
DATE = datetime.date(2021, 3, 17)
DB_PATH = "cheaters.db"

sqlite3.connect(DB_PATH)


def create_table(con: sqlite3.Connection):
    """
    Создание новой таблицы
    """
    sql = """
    create table if not exists merged(
        timestamp timestamp,
        player_id int,
        event_id text,
        error_id int,
        json_server text,
        json_client text
    )
    """
    cursor = con.cursor()
    cursor.execute(sql)
    con.commit()


def save_do_db(con: sqlite3.Connection, data: pd.DataFrame):
    """
    Сохранение результат в бд
    Parameters
    ----------
    con: подключение
    data: датафрейм для записи в таблицу
    """
    data.to_sql("merged", con, if_exists="replace")


def convert_date(time):
    return datetime.datetime.fromtimestamp(float(time))


def load_csv_on_date(first_path: str, second_path: str, date: datetime.date):
    """
    Чтение csv файлов и их фильтрация за указанную дату
    Parameters
    ----------
    first_path: путь к первому csv файлу
    second_path: путь ко второму csv файлу
    date: дата для фильтрации

    Returns
    -------
    Два соответствующих датафрейма
    """
    first = pd.read_csv(
        first_path, 
        header=0,
        parse_dates=[0], date_parser=convert_date, 
        names=["timestamp", "error_id", "player_id", "json_client"],
        usecols=["timestamp", "error_id", "player_id", "json_client"],
    )
    second = pd.read_csv(
        second_path,
        header=0,
        parse_dates=[0], date_parser=convert_date, 
        names=["timestamp", "event_id", "error_id", "json_server"],
        usecols=["timestamp", "event_id", "error_id", "json_server"],
    )
    
    return (first[first["timestamp"].dt.date == date],
            second[second["timestamp"].dt.date == date].drop(columns="timestamp"))


def get_merged_dataframe(client_path: str, server_path: str, date: datetime.date) -> pd.DataFrame:
    """
    'Склеивание' датафреймов по error_id
    Parameters
    ----------
    client_path: путь к первому csv файлу
    server_path: путь ко второму csv файлу
    date: дата

    Returns
    -------
    Конечный датафрейм
    """
    client_dataframe, server_dataframe = load_csv_on_date(client_path, server_path, date)
    return pd.merge(client_dataframe, server_dataframe, on="error_id")


def get_cheaters_on_date(connect: sqlite3.Connection, date: datetime.date) -> pd.DataFrame:
    """
    Получение из бд данных о читерах до указанной даты
    Parameters
    ----------
    connect: подключение
    date: дата

    Returns
    -------
    Датафрейм с данными из бд
    """
    query = """
        select player_id from cheaters
        where ban_time <= ?
    """
    return pd.read_sql_query(query, connect, params=[date - datetime.timedelta(days=1)])


def filter_cheaters(data: pd.DataFrame, cheaters: pd.DataFrame) -> pd.DataFrame:
    """
    Фильтрация конечного датафрейма на основе данных о читерах из бд
    Parameters
    ----------
    data: конечный датафрейм
    cheaters: список читеров из бд

    Returns
    -------
    Отфильтрованный датафрейм
    """
    return data[~data["player_id"].isin(cheaters["player_id"])]


@profile
def main():
    # Чтение из csv файлов за указанную дату и их слияние
    merged_df = get_merged_dataframe(CLIENT_PATH, SERVER_PATH, DATE)

    # Подключение к бд и создание новой таблицы
    con = sqlite3.connect("cheaters.db")
    create_table(con)

    # Получение необходимой инфы о читерах
    cheaters = get_cheaters_on_date(con, DATE)

    # Удаление читеров из датафрейма
    result = filter_cheaters(merged_df, cheaters)

    # Сохранение в бд
    save_do_db(con, result)
    con.close()


if __name__ == "__main__":
    main()
