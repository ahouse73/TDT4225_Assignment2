import os
import tabulate
from DbConnector import DbConnector
from decouple import config


def fetch_data(table_name):
    query = "SELECT COUNT(*) FROM %s;"
    cursor.execute(query % table_name)
    return cursor.fetchall()

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USER'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor
    
    amountOfUsers = fetch_data("User") # 182
    amountOfActivities = fetch_data("Activity") # 16048
    amountOfTrackpoints = fetch_data("TrackPoint") # 9681756
    

