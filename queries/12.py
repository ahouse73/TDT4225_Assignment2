import os
from tabulate import tabulate
from DbConnector import DbConnector
from decouple import config


def fetch_data():
    query = "select tr1.activity_id from TrackPoint as tr1 join TrackPoint as tr2 on tr1.activity_id = tr2.activity_id where tr1.id +1 = tr2.id and timestampdiff(SECOND, tr1.date_time, tr2.date_time) > 300;"
    cursor.execute(query)
    return cursor.fetchall()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USERNAME'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    result = fetch_data()
    print(tabulate(result, headers=["UserID", "Most used Transport Mode"]))
