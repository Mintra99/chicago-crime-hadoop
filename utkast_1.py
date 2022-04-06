import math
#       Python without MapReduce
import csv


"""
def getDistance(c, coord):
    x_y_dim = c.getDimensions()
    return cur_dist
"""

# how to pass the parameters in mapper step
# We can do run loop when calling hadoop
# it might not be necessary

def m_test(line):  
    dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
    line[-1] = ''.join(c for c in line[-1] if c not in '()')
    coords = line[-1].split(',')
    x_coord = float(coords[0])
    y_coord = float(coords[1])
    new_coord = [x_coord, y_coord]

    min_dist = math.inf
    closest_centroid = -1
    for c in dimensions:
        distance = math.sqrt(pow(new_coord[0]-c[0], 2) + pow(new_coord[1] - c[1], 2)) 
        if distance < min_dist:
            closest_centroid = dimensions.index(c)
            min_dist = distance
    val = [1, new_coord]
    return closest_centroid, val

def reducer(key, values):
    num_points = 0
    final_value = [0, 0]

    for v in values:
        # opdater punktteller
        point_num = values[0]
        num_points += point_num

        point_coord = values[1]
        # new_average_x = (self.num_points * self.x_y_dimensions[0] + coord[0]) / (self.num_points + 1)
        new_average_x = (num_points * final_value[0] + point_coord[0]) / (num_points + 1)
        new_average_y = (num_points * final_value[1] + point_coord[1]) / (num_points + 1)
        final_value = [new_average_x, new_average_y]
    return key, final_value

if __name__ == "__main__":
    file = open('python_preprocess.csv')
    csvreader = csv.reader(file)
    header = []
    header = next(csvreader)

    rows = []
    for row in csvreader:
        rows.append(row)
    rows
    file.close()

    mapper_result = []
    for row in rows:
        # mapper_result.append(
        key, val = m_test(row)
        print(str(key) + ", " + str(val))


# 1. Må kjøe MrJOben mange ganger med opdattert centroid
# 2. I reducer skal vi oppdate centroid
# 


# hello 5000 -> hello 200(1, 1, 1, ... 1) in dn1 and hello 300 in dn2
    
# combiner might work with small sets but values are different
# multiply dataset


"""
for m in mapper_result:
    reducer(m[0])
"""


"""
k=5
done = False
coords = getCoords()
centroids=getRandomCentroids(coords, k)
## print(centroids)
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
    i += 1
    # print(new_mean)
    # print(i)
    if new_mean == mean:
        
        done = True
    else:
        mean = new_mean
print(mean)
"""