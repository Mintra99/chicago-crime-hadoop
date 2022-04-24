import math
import numpy as np
#import findspark

# spark imports
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession



crimes_schema = StructType([StructField("ID", IntegerType(), True),
                            StructField("Date", StringType(), True ),
                            StructField("Primary Type", StringType(), True  ),
                            StructField("Latitude", DoubleType(), True),
                            StructField("Longitude", DoubleType(), True),
                            StructField("Location", StringType(), True )
                            ])

class spark_kmeans():
    def getDistance(self, centroid, point):
        cur_dist = math.sqrt((centroid[0]-point[0])**2 + (centroid[0]-point[0])**2) 
        return cur_dist    
        
    def map(self, pointList, centroids):
        centroid_dict = {}
        for point in pointList:
            min_dist = math.inf
            closest_centroid = -1
            for centroid in centroids:
                curr_dist = self.getDistance(centroid, point)                
                if curr_dist < min_dist:
                    min_dist = curr_dist
                    closest_centroid = centroid

            if tuple(closest_centroid) not in centroid_dict:
                centroid_dict[tuple(closest_centroid)] = list()
            centroid_dict[tuple(closest_centroid)].extend([point])
        return centroid_dict
 
    def reduce(self, mapped_data):
        new_centroid = []
        for centroid in mapped_data:
            X = []
            Y = []
            counter=0
            for point in mapped_data[centroid]:
                X.append(point[0])
                Y.append(point[1])
                counter += 1
            new_centroid.append([sum(X)/counter, sum(Y)/counter])
    
        return new_centroid


class WRCentroids():
    def retrieveCentroids(self, file):
        with open(file, "r") as inputFile:
            output_data = inputFile.readlines()
        centroids = []
        for line in output_data:
            line = line.split(';')
            untreated_str_coords = line[1]
            str_coords = ''.join(c for c in untreated_str_coords if c not in '[ ]')
            coords = str_coords.split(',')
            try:
                x_coord = float(coords[0].strip())
            except ValueError:
                x_coord = float(0)
            
            try:
                y_coord = float(coords[1].strip())
            except ValueError:
                y_coord = float(0)
            
            new_coord = [x_coord, y_coord]

            if len(new_coord) == 2:
                centroids.append(new_coord)
        return centroids

    def writeCentroids(self, centroids, file):
        f = open(file, "w+")
        iteration = 1
        for coord in centroids:
            string_item = str(iteration) + ' ; ' + str(coord) + ' ; 1'
            f.write("%s\n" % string_item)
            iteration += 1
        f.close()
    
    def badWrite(self, centroids, file):
        f = open(file, "w+")
        for item in centroids:
            string_item = ''.join(c for c in str(item) if c not in "[]")
            f.write("%s\n" % string_item)
        f.close()
    
    def checkCloseness(a, b, rel_tol=1e-05, abs_tol=0.0):
        return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)
    
    
    # Spark exclusive
    def convertPoints(self, points):
        coordList = []
        i = 0
        for coord in points.collect():
            # to skip column name
            if i == 0:
                i+=1
                continue
            cleaned_coord = ''.join(c for c in coord["Location"] if c not in '()')
            new_coords = cleaned_coord.split(',')
            x_coord = float(new_coords[0])
            y_coord = float(new_coords[1])
            coords = [x_coord, y_coord]
            coordList.append(coords)
        return coordList

if __name__ == "__main__":
    """
    install_requires=[
        'pyspark=={site.SPARK_VERSION}'
    ]
    """

    wrCentroid = WRCentroids()
    kmeans = spark_kmeans()

    conf = SparkConf().setAppName("PySPark CrimesChicago").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    #findspark.init()
    
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    distFile = sc.textFile("data.txt")
    
    
    ###############
    rdd = spark.read.csv('hdfs://namenode:9000/DAT500/spark_preprocess.csv', schema=crimes_schema)
    points = rdd.select("Location")
    centroids = wrCentroid.retrieveCentroids('starting_centroid.txt')
    # points.show(5)
    ###############
    
    # first run
    pointList = wrCentroid.convertPoints(points)
    mapped_points = kmeans.map(pointList, centroids)
    new_centroids = kmeans.reduce(mapped_points)
    
    # after first run
    while True:
        min_dist = 0.0001
        done = True
        for i in range(len(new_centroids)):
            distance = math.sqrt(pow(centroids[i][0]-new_centroids[i][0], 2) + pow(centroids[i][1] - new_centroids[i][1], 2)) 
            if distance > min_dist:
                done = False

        if done:
            print(new_centroids)
            break
        else:
            centroids = new_centroids
            mapped_points = kmeans.map(pointList, centroids)
            new_centroids = kmeans.reduce(mapped_points)

    """
    i = 1 # + " --centroids=" \ # mellom data og files
    #while True:
        # print("--Iteration n. {itr:d}".format(itr=n+1), end="\r", flush=True)

        
        # reducer: for each cluster, compute the sum of the points belonging to it. 
        # It is mandatory to pass one associative function as a parameter. 
        # The associative function (which accepts two arguments and returns a single element) 
        # should be commutative and associative in mathematical nature

    while True:
      

        new_centroids = wrCentroid.retrieveCentroids(output)
        print("NEW CENTROIDS: " + str(new_centroids))

        min_dist = 0.0001
        done = True
        for i in range(len(new_centroids)):
            distance = math.sqrt(pow(centroids[i][0]-new_centroids[i][0], 2) + pow(centroids[i][1] - new_centroids[i][1], 2)) 
            if distance > min_dist:
                done = False

        if done:
            break
        else:
            centroids = new_centroids
            if len(centroids) != 0:
                wrCentroid.writeCentroids(centroids, file)
        i +=1
        # https://spark.apache.org/docs/latest/rdd-programming-guide.html
    """