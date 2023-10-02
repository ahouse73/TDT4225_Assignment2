import os
from tabulate import tabulate
from DbConnector import DbConnector
from haversine import haversine, Unit


def get_transport_modes():
	query = "SELECT DISTINCT transportation_mode FROM Activity WHERE transportation_mode != '';"
	cursor.execute(query)
	rows = cursor.fetchall()
	return rows

def get_activities_for_transport_mode(transportation_mode):
    query = "SELECT id, user_id FROM Activity WHERE transportation_mode = '%s' AND DATEDIFF(start_date_time, end_date_time) = 0;"
    cursor.execute(query % transportation_mode)
    rows = cursor.fetchall()
    return rows

def get_total_distance_for_activity(activity_id):
	query = "SELECT lat, lon FROM TrackPoint WHERE activity_id = %s;"
	cursor.execute(query % activity_id)
	rows = cursor.fetchall()

	totaldistance = 0
	prev_point = rows[0]
	for point in rows:
		if point == prev_point:
			continue

		totaldistance += haversine(point, prev_point)
		prev_point = point

	return totaldistance

def get_user_for_activity(activity_id):
    query = "SELECT user_id FROM Activity WHERE id = %s;"
    cursor.execute(query % activity_id)
    rows = cursor.fetchall()
    
    return rows
         


if __name__ == '__main__':
	current_dir = os.path.dirname(os.path.abspath(__file__))
	dataset_base_path = os.path.join(current_dir, 'dataset', 'dataset')

	connection = DbConnector(HOST=config('HOST'), DATABASE=config('DATABASE'), USER=config('USERNAME'), PASSWORD=config('PASSWORD'))
	db_connection = connection.db_connection
	cursor = connection.cursor
		
	transport_modes = get_transport_modes()
	users = []

	for mode, *_ in transport_modes:
		activities = get_activities_for_transport_mode(mode)

		total_distance = {}

		for act in activities:
			(act_id, user_id) = act
			distance = get_total_distance_for_activity(act_id)
			user_id = str(user_id)
			if user_id in total_distance:
				total_distance[user_id] += distance
			else:
				total_distance[user_id] = distance
		Keymax = max(total_distance, key=lambda x: total_distance[x])
		print(Keymax)	

# 062 128 062 128 128 062 062 128 128 128
