####### A possible algorithm to solve the coding test about train stations
####### PSEUDO CODE!

# Main function that read inputs and call the recursive function to find the required time

def FindPath():
	import sys
	data = sys.stdin.readlines()

	# Parsing the first table that contains the travel time between connected stations
    # Create a list(dimension N) named Timetable, of arrays(dimension 3) for each line in the input

    Timetable = [array([start_station_1, arrival_station_1, travel_time_1]),
                 array([start_station_2, arrival_station_2, travel_time_2]),
                 # ...
				 ]

	# Parsing the second table containing the pairs of stations among with we will calculate the integrated travel time
    # Create a list(dimension M) named MyTrip, of arrays(dimension 2) for each pair of stations

    MyTrip = [array([departure_station_1, destination_station_1]),
			  array([departure_station_2, destination_station_2]),
              # ...
			  ]

	for (departure_station, destination_station) in MyTrip:
		cumulative_travel_time = 0
		last_station = null
		return TimePath(Timetable,
						departure_station,
						destination_station,
						last_station,
						cumulative_travel_time)

   
### This function is the recursive function that calculates the travel time ###

def TimePath(Timetable, departure_station, destination_station, last_station, cumulative_travel_time=0):

	## Check if exist a direct connection between dep and dest or find in the timetable all possible
	## connection with my departure point

	for (start_station, arrival_station, travel_time) in Timetable:

		if start_station == departure_station:
			if arrival_station == destination_station:
				return cumulative_travel_time + travel_time

			elif arrival_station == last_station:
				return 0

			else:
				new_departure = arrival_station
				cumulative_travel_time += travel_time

				# save the last station where you are in order not to move back
				last_station = start_station

				#Call same func (recursive) with as new departure point, the arrival of previous step
				return TimePath(Timetable,
								departure_station=new_departure,
								destination_station=destination_station,
								last_station=last_station,
								cumulative_travel_time=cumulative_travel_time)

					


















