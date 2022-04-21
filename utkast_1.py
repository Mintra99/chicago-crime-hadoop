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

"""
1. Chaining
Combining multiple mappers and reducers to complete a job

2. MrStep

"""
    

def mapper(line):  
    dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
    try:
        x_coord = float(line[-2])
    except ValueError:
        x_coord = float(0)

    try:
        y_coord = float(line[-1])
    except ValueError:
           y_coord = float(0)
 
    new_coord = [x_coord, y_coord]

    min_dist = math.inf
    closest_centroid = -1
    for c in dimensions:
        distance = math.sqrt(pow(new_coord[0]-c[0], 2) + pow(new_coord[1] - c[1], 2)) 
        if distance < min_dist:
            closest_centroid = dimensions.index(c)
            min_dist = distance
    # val = [1, new_coord]
    return closest_centroid, new_coord

"""
return [MRStep(mapper=self.assignPointtoCluster,
        combiner=self.calculatePartialSum,
        reducer=self.calculateNewCentroids)
        ]
"""


"""
You will provide the next epoch of K-Means with:
the same data from your initial epoch
the centers emitted from the reducer as global constants
Repeat until your stopping criteria are met.
"""

def reducer(key, values):
    dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]


    final_value = dimensions[(key-1)]
    num_points = 0

    for v in values: #[2:]:
        # opdater punktteller
        num_points += 1

        # new_average_x = (self.num_points * self.x_y_dimensions[0] + coord[0]) / (self.num_points + 1)
        new_average_x = (num_points * final_value[0] + v[0]) / (num_points + 1)
        new_average_y = (num_points * final_value[1] + v[1]) / (num_points + 1)
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
    print(len(rows))
    value = 492817

    dn1_map = {}
    dn2_map = {}
    dn3_map = {}
    for row in rows[:value]:
        # mapper_result.append(
        key, val = mapper(row)
        if key not in dn1_map:
            value = [val]
            dn1_map[key] = value
        else:
            value = dn1_map[key]
            value.append(val)
            dn1_map[key] = value
    print(dn1_map.keys())
        
    for i in range(4):
        centroid, new_coords = reducer(i, dn1_map[i])
        print(str(centroid) + ", " + str(new_coords))
        
    # print(dn1_map)
    """
    for row in rows[(value+1):(value*2)]:
        # mapper_result.append(
        key, val = mapper(row)
        # print(str(key) + ", " + str(val))
        if key not in dn2_map:
            value = [val]
            dn2_map[key] = value
        else:
            value = dn2_map[key]
            value.append(val)
            dn2_map[key] = value
        for i in range(1, 4):
            centroid, new_coords = reducer(i, dn2_map[i])
    for row in rows[((value*2)+1):]:
        # mapper_result.append(
        key, val = mapper(row)
        # print(str(key) + ", " + str(val))
        if key not in dn3_map:
            value = [val]
            dn3_map[key] = value
        else:
            value = dn3_map[key]
            value.append(val)
            dn3_map[key] = value
        for i in range(1, 4):
            centroid, new_coords = reducer(i, dn3_map[i])
    """

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