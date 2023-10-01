import os
import tabulate
from DbConnector import DbConnector
from decouple import config


def fetch_data():
    query = "SELECT sub.user_id, SUM(sub.altitude_gain) FROM (SELECT act1.user_id, (tr2.altitude - tr1.altitude) as altitude_gain FROM Activity as act1 JOIN TrackPoint as tr1 on act1.id = tr1.activity_id join TrackPoint as tr2 on tr1.activity_id=tr2.activity_id join Activity as act2 on tr2.activity_id = act2.id WHERE tr1.altitude != -777 and tr2.altitude != -777 and tr2.altitude > tr1.altitude and tr1.id +1 = tr2.id ) as sub group by sub.user_id order by SUM(sub.altitude_gain) desc limit 15;"
    cursor.execute(query)
    return cursor.fetchall()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USER'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    fetch_data()

