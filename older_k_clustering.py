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

# 1 distansen mellom alle punktene i et cluster og midten
# 2 finn midten til alle clustrene


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
    point_index = []
    for _ in range(k):
        point_index.append(coords[random.randint(0,len(coords)-1)])
    print(point_index)
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
        # line[-1] = ''.join(c for c in line[-1] if c not in '()')
        # coords = line[-1].split(',')

        try:
            x_coord = float(line[-2].strip())
        except ValueError:
            x_coord = float(0)
        
        try:
            y_coord = float(line[-1].strip())
        except ValueError:
            y_coord = float(0)
        new_coord = [x_coord, y_coord]
        allcoord.append(new_coord)
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

#       Kommentert noe ut for ?? kj??re filen i python
#     def mapper(self, _, line):
# line = line.strip() # remove leading and trailing whitespace
# l_array = line.split(';')
# points = l_array[10]
# line = line.strip() # remove leading and trailing whitespace
# yield line, 1

# [[41.899082422, -87.71917838], [41.941161268, -87.642667917], [41.960447836, -87.669222376], [41.883224344, -87.624971297], [41.852589811, -87.713647735]]

#    def combiner(self, key, values):
#        yield key, sum(values)
        
#    def reducer(self, key, values):
#         yield key, sum(values)

if __name__ == "__main__":
    for _ in range(20):
        k=5
        done = False
        coords = getCoords()
        print("STARTING CENTROIDS: ")
        centroids=getRandomCentroids(coords, k)

        # recalculate cluster representative hver gang vi leger til noe nytt i cluster
        # then iterate the dataset again, compute the sim between
        # each element and its curent cluster
        mapper(centroids, coords, k)
        mean = reducer(centroids)
        # print(mean)
        i = 1
        while done == False:
            centroids = getCentroids(mean)
            mapper(centroids, coords, k)
            new_mean = reducer(centroids)
            # print(new_mean)
            i += 1
            """
            print(i)
            if i > 20:
                done = True
            """
            finished = True
            min_dist = 0.0001
            for j in range(len(new_mean)):
                distance = math.sqrt(pow(mean[j][0]-new_mean[j][0], 2) + pow(mean[j][1] - new_mean[j][1], 2)) 
                if distance > min_dist:
                    finished = False
            
            if finished == True:
                done = True
                print("DONE")
                print(new_mean)
                print(i)
            else:
                mean = new_mean



    # var_list = []
    # sims = 100
    # for _ in range(sims):
    #     centroids = mapper()
    #     var = reducer(centroids)
    #     var_list.append(var)
    #     centroids = []
    
    # best_x_y = 1000
    # for var in var_list:
    #     total_var = var[0] + var[1]
    #     if total_var < best_x_y:
    #         best_x_y = total_var
    # print(best_x_y)

    
    #MRCountSum.run()