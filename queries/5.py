import os
from tabulate import tabulate
from DbConnector import DbConnector
from decouple import config


def fetch_data():
    query = "select sub.user_id, count(sub.transportation_mode) from (select distinct user_id, transportation_mode from Activity) as sub group by sub.user_id order by count(sub.transportation_mode) desc limit 10;"
    cursor.execute(query)
    return cursor.fetchall()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USERNAME'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor

    result = fetch_data()
    print(tabulate(result, headers=["UserID", "Amount of Different Modes"]))
