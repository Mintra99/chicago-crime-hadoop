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

csvreader = csv.reader(file)

header = []
header = next(csvreader)
rows = []
for row in csvreader:
    rows.append(row)
rows
file.close()

# 1 get coords
# 2 get centroids
# 3 assign every object to the cluster it is most similar to
# NEW: Use Euclidean distance to calculate how similar these
#       objects are
#       therefore turn everything to a number
# 4 NEW everytime a new memeber joins a cluster recalculate
#   the cluster representatives
#   Categorize each data items to its closest centroid and update th
#   centroid coordinates calculating the average of items coordinates
#   categorized in that group so far

# SO we update the center running, instead of iteratively

def mapper(centroids, coords, k=3):    
    for coord in coords:
        min_dist = math.inf
        closest_centroid = -1
        for c in centroids:
            distance = getDistance(c, coord)
            if distance < min_dist:
                closest_centroid = centroids.index(c)
                min_dist = distance
        # c.add_coord(coord)
        c.add_to_average(coord)


def getDistance(c, coord):
    x_y_dim = c.getDimensions()
    cur_dist = math.sqrt(pow(coord[0]-x_y_dim[0], 2) + pow(coord[1] - x_y_dim[1], 2))
    return cur_dist

def getRandomCentroids(coords, k=3):
    centroids = []
    # print(len(coords))
    point_index = []
    i_list = [784724, 1108647, 1458107, 495697, 1378509]
    for i in range(k):
        # value = random.randint(0,len(coords)-1)
        value = i_list[i]
        print("VALUE: " + str(value))
        point_index.append(coords[value])
    # print(point_index)
    for i in range(k):
        p = Point([], point_index[i])
        centroids.append(p)
    return centroids

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
    return allcoord

"""
def reducer(centroids):
    new_centroids = []
    for c in centroids:
        print(c.getDimensions())
        new_mean = c.average()
        new_centroids.append([new_mean[0], new_mean[1]])
    return new_centroids
"""

def reducer_variation():
    centroid_variation = []
    for c in centroids:
        variation = c.variation()
        centroid_variation.append(variation)
    return centroid_variation


if __name__ == "__main__":
    k=5
    done = False
    coords = getCoords()
    centroids=getRandomCentroids(coords, k)

    mapper(centroids, coords, k)
    for c in centroids:
        print("c get dimensions_ " + str(c.getDimensions()))
    # mean = reducer(centroids)
    """
    i = 1
    while done == False:
        centroids = getCentroids(mean)
        mapper(centroids, coords, k)
        new_mean = reducer(centroids)
        i += 1
        if new_mean == mean:
            done = True
        else:
            mean = new_mean
    """
