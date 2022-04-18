from unittest import result
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
def mapper(centroids, coords, k=3):
    
    # start the algorithm:
    for coord in coords:
        min_dist = math.inf
        closest_centroid = -1
        for c in centroids:
            distance = getDistance(c, coord)
            if distance < min_dist:
                closest_centroid = centroids.index(c)
                min_dist = distance
        centroids[closest_centroid].add_coord(coord)

# when executing in mapreduce specify as inline instead of hadoop    
""" # much faster than hadoop, inline and local
# inline: everything within one container, one jvm
# local: run in multiple jvm but in one machine
# inline show python error, some shared state will not be detected
# FIX inline, FIX local, then hadoop
#Testing code without any Hadoop installation
cat hadoop_1m.txt | ./email_count_mapper.py | sort -k1,1 | ./email_count_reducer.py
python count_sum.py -r inline hadoop_1m.txt
"""

def getDistance(c, coord):
    x_y_dim = c.getDimensions()
    cur_dist = math.sqrt(pow(coord[0]-x_y_dim[0], 2) + pow(coord[1] - x_y_dim[1], 2))
    return cur_dist
"""
min_distance = POSITIVE_INFINITY

for all centroid in list_of_centroids
    distance = distance(centroid, point)
    if (distance < min_distance)
        closest_centroid = index_of(centroid)
        min_distance = distance
EMIT(closest_centroid, point)
"""

def getRandomCentroids(coords, k=3):
    centroids = []
    # print(len(coords))
    point_index = []
    i_list = [784724, 1108647, 1458107, 495697, 1378509]
    for i in range(k):
        # value = random.randint(0,len(coords)-1)
        value = i_list[i]
        print(value)
        point_index.append(coords[value])
    # print(point_index)
    for i in range(k):
        p = Point([], point_index[i])
        centroids.append(p)
    return centroids

"""
def getRandomCentroids(coords, k=3):
    centroids = []
    point_index = []
    for _ in range(k):
        point_index.append(coords[random.randint(0,len(coords)-1)])
    
    for i in range(k):
        p = Point([], point_index[i])
        centroids.append(p)
    return centroids
"""

def getCentroids(list):
    centroids = []
    for coord in list:
        p = Point([], coord)
        centroids.append(p)
    return centroids

def getCoords():
    allcoord = []
    for line in rows:
        line[-1] = ''.join(c for c in line[-1] if c not in '()')
        coords = line[-1].split(',')
        x_coord = float(coords[0])
        y_coord = float(coords[1])
        new_coord = [x_coord, y_coord]
        allcoord.append(new_coord)
        '''
        TH sin kode, funker ikke på min csv
        '''
        x_coord = line[0].split('M;(')
        
        # # x_coord = x_coord[len(x_coord)-1]
        # if len(x_coord) > 1:
        #     x_coord = float(x_coord[1])
        #     y_coord = line[1]
        #     y_coord = float(y_coord[1:len(y_coord)-1])
        #     coord = [x_coord, y_coord]
        #     allcoord.append(coord)
    return allcoord


def reducer(centroids):
    new_centroids = []
    for c in centroids:
        new_mean = c.average()
        # original = c.getDimensions()
        # var_x = abs(new_mean[0]-original[0])
        # var_y = abs(new_mean[1]-original[1])
        new_centroids.append([new_mean[0], new_mean[1]])
    return new_centroids

def reducer_variation():
    centroid_variation = []
    for c in centroids:
        variation = c.variation()
        centroid_variation.append(variation)
    return centroid_variation
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

"""
method COMBINER(centroid_index, list_of_points)
    point_sum.number_of_points = 0
    point_sum = 0
    for all point in list_of_points:
        point_sum += point
        point_sum.number_of_points += 1
    EMIT(centroid_index, point_sum)    
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
    k=5
    done = False
    coords = getCoords()
    centroids=getRandomCentroids(coords, k)
    ## print(centroids)
    # recalculate cluster representative hver gang vi leger til noe nytt i cluster
    # then iterate the dataset again, compute the sim between
    # each element and its curent cluster

    mapper(centroids, coords, k)
    """
    for c in centroids:
        print(c.getDimensions())
    """
    mean = reducer(centroids)
    # print(mean)
    i = 1
    while done == False:
        centroids = getCentroids(mean)
        mapper(centroids, coords, k)
        new_mean = reducer(centroids)
        i += 1
        # print(new_mean)
        # print(i)
        if new_mean == mean:
            
            done = True
        else:
            mean = new_mean
    print(mean)
    
    # 1. Create k clusters
    # 2. Select window lengths for training and test data respectively,
    # select a test period and training period

    # Data Mining:
    # 1. Create N training series
    # 2. Normalise the series so that the first values of the series fall
    # between 0 and 1
    # 3. partition the data into k clusters, which are represented by the
    # centers.
    # 4. Classify the clusters into three distinct classes using a linear
    # regression model

    # Test models on test data
    # 1. Form a test series dataset and normalize it
    # 2. Assign a cluster label to time series i in test data so that
    # cluster j has the smalles Euclidean distance to the normalized series i
    # 3. Assign the class k=3 ("UP", "DOWN", "HOLD") of cluster j to time
    # series i, where time series i has cluter label j
    # 4. Calculate returns for a selected trading strategy