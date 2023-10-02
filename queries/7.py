import os
from tabulate import tabulate
from DbConnector import DbConnector
from decouple import config


def fetch_data_a():
    query = "SELECT COUNT(DISTINCT(user_id)) FROM Activity WHERE DATEDIFF(end_date_time, start_date_time) = 1;";
    cursor.execute(query)
    return cursor.fetchall()

def fetch_data_b():
    query = "select transportation_mode, user_id, timestampdiff(MINUTE, start_date_time, end_date_time) FROM Activity where to_days(start_date_time) + 1 = to_days(end_date_time);"
    cursor.execute(query)
    return cursor.fetchall()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USERNAME'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    fetch_data_a()
    fetch_data_b()


    result_a = fetch_data_a()
    print(tabulate(result_a, headers=["Number Of Users"]))
    
    result_b = fetch_data_b()
    print(tabulate(result_b[:10], headers=["Mode", "UserID", "Duration"]))
