import os
import tabulate
from DbConnector import DbConnector
from decouple import config


def fetch_data():
    query = "SELECT avg(sub.cnt), max(sub.cnt), min(sub.cnt) FROM (select a.user_id, count(t.id) as cnt FROM TrackPoint AS t join Activity as a on t.activity_id = a.id GROUP BY a.user_id) as sub;"
    cursor.execute(query)
    return cursor.fetchall()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USERNAME'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    result = fetch_data()[0]
    
    average = result[0]
    maximum = result[1]
    minimum = result[2]
    
    print(f"Average Trackpoints per user: {average}")
    print(f"Maximum Trackpoints per user: {maximum}")
    print(f"Minimum Trackpoints per user: {minimum}")

