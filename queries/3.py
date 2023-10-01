import os
import tabulate
from DbConnector import DbConnector
from decouple import config


def fetch_data():
    query = "SELECT user_id, COUNT(*) AS Amount FROM Activity GROUP BY user_id ORDER BY Amount DESC LIMIT 15;"
    cursor.execute(query)
    return cursor.fetchall()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USER'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    fetch_data()

