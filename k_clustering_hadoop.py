from mrjob.job import MRJob
from k_clustering_point_class import Point
import math
import random
#       MapReduce
# class MRCountSum(MRJob):

#       Python without MapReduce
import csv
file = open('python_preprocess.csv')
# type(file)
#       filtypen er "_io.TextIOWrapper"
#       Vi vill lese som csv
csvreader = csv.reader(file)

#       HEADER:
# Date;Block;Primary Type;Description;Arrest;Domestic;Beat;Community Area;Year;Updated On;Location
# 0     1           2          3        4       5       6           7       8       9       10
header = []
header = next(csvreader)
#       ROWS er de faktiske verdiene
rows = []
for row in csvreader:
        rows.append(row)
rows
file.close()



"""
- Given a point and the set of centroids.
- Calculate the distance between the point and each centroid.
- Emit the point and the closest centroid.

class MAPPER
method MAP(file_offset, point)
    min_distance = POSITIVE_INFINITY
    closest_centroid = -1
    for all centroid in list_of_centroids
        distance = distance(centroid, point)
        if (distance < min_distance)
            closest_centroid = index_of(centroid)
            min_distance = distance
    EMIT(closest_centroid, point) 

"""
def mapper(k=3):
    coords = getCoords()

    point_index = []
    for _ in range(k):
        point_index.append(random.randint(0,len(coords)))

    centroids = []
    for i in range(k):
        p = Point([], )
        centroids.append(p)
    # print(centroids)

    # start the algorithm:
    min_dist = math.inf
    closest_centroid = -1
    """
    min_distance = POSITIVE_INFINITY
    
    for all centroid in list_of_centroids
        distance = distance(centroid, point)
        if (distance < min_distance)
            closest_centroid = index_of(centroid)
            min_distance = distance
    EMIT(closest_centroid, point)
    """


def getCoords():
    allcoord = []
    for line in rows:
        x_coord = line[0].split('M;(')
        # x_coord = x_coord[len(x_coord)-1]
        if len(x_coord) > 1:
            x_coord = float(x_coord[1])
            # print(x_coord)
            y_coord = line[1]
            y_coord = float(y_coord[1:len(y_coord)-1])
            # print(y_coord)
            coord = [x_coord, y_coord]
            allcoord.append(coord)
    return allcoord

"""
method COMBINER(centroid_index, list_of_points)
    point_sum.number_of_points = 0
    point_sum = 0
    for all point in list_of_points:
        point_sum += point
        point_sum.number_of_points += 1
    EMIT(centroid_index, point_sum)    
"""

"""
- Given the centroid and the points belonging to its cluster.
- Calculate the new centroid as the aritmetic mean position of the 
- Emit the new centroid.      

class REDUCER
    method REDUCER(centroid_index, list_of_point_sums)
        number_of_points = partial_sum.number_of_points
        point_sum = 0
        for all partial_sum in list_of_partial_sums:
            point_sum += partial_sum
            point_sum.number_of_points += partial_sum.number_of_points
        centroid_value = point_sum / point_sum.number_of_points
        EMIT(centroid_index, centroid_value)
"""

#       Kommentert noe ut for å kjøre filen i python
#     def mapper(self, _, line):
# line = line.strip() # remove leading and trailing whitespace
# l_array = line.split(';')
# points = l_array[10]
# line = line.strip() # remove leading and trailing whitespace
# yield line, 1



#    def combiner(self, key, values):
#        yield key, sum(values)
        
#    def reducer(self, key, values):
#         yield key, sum(values)

if __name__ == "__main__":
    mapper()
    #MRCountSum.run()