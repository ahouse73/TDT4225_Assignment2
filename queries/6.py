import os
import tabulate
from DbConnector import DbConnector
from decouple import config


def fetch_data():
    query = "select * from (select user_id, id, start_date_time, end_date_time from Activity) as sub JOIN Activity as a on sub.start_date_time = a.start_date_time and sub.end_date_time = a.end_date_time and sub.user_id = a.user_id WHERE sub.id != a.id; "
    cursor.execute(query)
    return cursor.fetchall()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USER'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    fetch_data()

