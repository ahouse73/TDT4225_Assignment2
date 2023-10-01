import os
import tabulate
from DbConnector import DbConnector
from decouple import config


def fetch_data():
    query = "SELECT act1.user_id, act1.id, tr1.id, act2.user_id, act2.id, tr2.id, st_distance_sphere(point(tr1.lon, tr1.lat), point(tr2.lon, tr2.lat)) as distance, timestampdiff(SECOND, tr1.date_time, tr2.date_time) as time_diff FROM Activity as act1 JOIN TrackPoint as tr1 on act1.id = tr1.activity_id JOIN TrackPoint as tr2 JOIN Activity as act2 ON tr2.activity_id = act2.id WHERE act1.id != act2.id and st_distance_sphere(point(tr1.lon, tr1.lat), point(tr2.lon, tr2.lat)) <= 50 and timestampdiff(SECOND, tr1.date_time, tr2.date_time) between -30 and 30;"
    cursor.execute(query)
    return cursor.fetchall()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USER'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    fetch_data()

