import pandas as pd
import os
from DbConnector import DbConnector
from tabulate import tabulate
from decouple import config

def drop_all_tables():
    query = """DROP TABLE IF EXISTS TrackPoint, Activity, User 
                """
    # This adds table_name to the %s variable and executes the query
    cursor.execute(query)
    db_connection.commit()

def create_user_table():
    query = """CREATE TABLE IF NOT EXISTS User (
                   id VARCHAR(255) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
    # This adds table_name to the %s variable and executes the query
    cursor.execute(query)
    db_connection.commit()

def create_activity_table():
    query = """CREATE TABLE IF NOT EXISTS Activity (
                   id int NOT NULL PRIMARY KEY,
                   user_id VARCHAR(255),
                   transportation_mode VARCHAR(255),
                   start_date_time DATETIME,
                   end_date_time DATETIME,
                   FOREIGN KEY (user_id) REFERENCES User(id))
                """
    # This adds table_name to the %s variable and executes the query
    cursor.execute(query)
    db_connection.commit()

def create_trackpoint_table():
    query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                   id int PRIMARY KEY AUTO_INCREMENT,
                   activity_id int,
                   lat DOUBLE,
                   lon DOUBLE,
                   altitude int,
                   date_days DOUBLE,
                   date_time DATETIME,
                   FOREIGN KEY (activity_id) REFERENCES Activity(id))
                """
    # This adds table_name to the %s variable and executes the query
    cursor.execute(query)

def get_all_user_data(data_path):
    # create for each user entry in list to insert them all in once
    all_users = os.listdir(data_path)
    # transform it in tuple with (id, has_labels) set has_labels to False for all
    all_users = [(u, 0) for u in all_users]
    with open(os.path.join(dataset_base_path, 'labeled_ids.txt')) as f:
        for line in f.readlines():
            # update all entries with idx in file to has_labels = True
            all_users[int(line.strip())] = (all_users[int(line.strip())][0], 1)
    
    return all_users


def upload_single_activity(data):
    query = """INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time)
    VALUES (%s, %s, %s, %s, %s)"""
    cursor.execute(query, data)
    db_connection.commit()

def upload_users_batch(data):
    cursor.executemany("""INSERT INTO User (id, has_labels)
    VALUES (%s, %s)""", data)
    db_connection.commit()

def upload_trackpoints_batch(data):
    # format date 
    formatted_data = []
    for d in data:
        formatted_data.append((d[0], d[1], d[2], d[3], d[4], d[5].strftime('%Y-%m-%d %H:%M:%S')))
    cursor.executemany("""INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)
    VALUES (%s, %s, %s, %s, %s, %s)""", formatted_data)
    db_connection.commit()

def get_trackpoints_as_df(plt_file_path):
    trackpoints = pd.read_csv(
    plt_file_path,
    skiprows=6,
    header=None,
    parse_dates=[[5, 6]],
    keep_date_col=True
    )
    # rename attributes 
    trackpoints = trackpoints.rename(columns={
        "5_6": "Date",
        0: "Latitude",
        1: "Longitude",
        3: "Altitude",
        4: "date_days",
        6: "date_time"
    })
    # drop column 2 (all zeros) and 5 (date redundant)
    return trackpoints.drop(columns=[2, 5])

def get_labels_as_df(file_path):
    temp = pd.read_csv(file_path, sep='\t', parse_dates=['Start Time','End Time'], date_format='yyyy/mm/dd hh:mm:ss')
    temp['Start Time'] = pd.to_datetime(temp['Start Time'])
    temp['End Time'] = pd.to_datetime(temp['End Time'])
    return temp

if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

    connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USER'), PASSWORD=config('PASSWORD'))
    db_connection = connection.db_connection
    cursor = connection.cursor

    drop_all_tables()
    create_user_table()
    create_activity_table()
    create_trackpoint_table()
    all_users = get_all_user_data(os.path.join(dataset_base_path, 'Data'))
    upload_users_batch(all_users)

    current_activity_id = 0
    for user in os.listdir(os.path.join(dataset_base_path, 'Data')):
        print("User", user)
        base_path = os.path.join(dataset_base_path, 'Data', user)
        labels_exists = os.path.exists(os.path.join(base_path, 'labels.txt'))
        if labels_exists:
            df_labels = get_labels_as_df(os.path.join(base_path, 'labels.txt'))
        for activity in os.listdir(os.path.join(base_path, 'Trajectory')):
            path_activity = os.path.join(base_path,'Trajectory',activity)
            activity_df = get_trackpoints_as_df(path_activity)
            if activity_df.shape[0] <= 2500:
                start_time = activity_df.head(1).values[0][0]
                end_time = activity_df.tail(1).values[0][0]
                transportation_label = ''
                if labels_exists:
                    matching_row = df_labels.loc[(df_labels['Start Time'] == start_time) & ((df_labels['End Time']) == end_time)]
                    if matching_row.shape[0] > 0:
                        transportation_label = matching_row.values[0][2]
                
                upload_single_activity((current_activity_id, user, transportation_label, start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S')))
                
                activity_df.drop(columns=['date_time'], inplace=True)
                activity_df['activity_id'] = current_activity_id
                activity_df = activity_df.reindex(columns=['activity_id', 'Latitude', 'Longitude', 'Altitude', 'date_days', 'Date'])
                upload_trackpoints_batch(list(activity_df.itertuples(index=False, name=None)))
            
            current_activity_id += 1