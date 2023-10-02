import os
from tabulate import tabulate
from DbConnector import DbConnector
from decouple import config


def fetch_data():
    query = "SELECT DISTINCT user_id FROM Activity WHERE transportation_mode='Bus';"
    cursor.execute(query)
    return cursor.fetchall()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USERNAME'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    result = fetch_data()
    print(tabulate(result, headers=["UserID"]))

