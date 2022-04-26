import math
import time

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
    
    def writeTime(self, time_list):
        f = open('time_list_file.txt', "w+")
        string_item = 'start: ' + str(time_list[0]) + ' | end: ' + str(time_list[1]) + ' | run time: ' + str(time_list[2])
        f.write("%s\n" % string_item)
        f.close()
    
    def create_file(self, i, centroids):
        name = "start_c_" + str(i) + ".txt"
        f = open(name, "w")
        iteration = 1
        for item in centroids:
            string_item = str(iteration) + ' ; ' + str(item) + ' ; 1'
            f.write("%s\n" % string_item)
        f.close()
    
    def createTimeFile(self, i, time_list):
        name = "TIME_start_c_" + str(i) + ".txt"
        f = open(name, "w")
        string_item = 'start: ' + str(time_list[0]) + ' | end: ' + str(time_list[1]) + ' | run time: ' + str(time_list[2])
        f.write("%s\n" % string_item)
        f.close()

if __name__ == "__main__":

    conf = SparkConf().setAppName("PySPark CrimesChicago").setMaster("local[*]")
    sc = SparkContext(conf=conf)

        #findspark.init()
        
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    

    wrCentroid = WRCentroids()
    starting_centroids = [[[41.812842218, -87.728659989], [41.909408388, -87.675949324], [41.705169694, -87.63708421], [41.898916021, -87.732333607], [41.744235532, -87.551407988]],
    [[41.899082422, -87.71917838], [41.941161268, -87.642667917], [41.960447836, -87.669222376], [41.883224344, -87.624971297], [41.852589811, -87.713647735]],
    [[41.910008891, -87.715396172], [41.93688984, -87.721778097], [41.747672195, -87.601090224], [41.848229383, -87.633428407], [41.77514011, -87.590017895]],
    [[41.760735939, -87.647937849], [41.742372451, -87.637668133], [41.707250544, -87.604006449], [41.924672785, -87.711094988], [41.930414054, -87.762393371]],
    [[41.88115467, -87.687240771], [41.891867685, -87.616406419], [41.791367197, -87.687633502], [42.000413228, -87.670455154], [41.924656143, -87.712583382]],
    [[41.855497329, -87.699810548], [41.846596134, -87.68478092], [41.877264269, -87.711775408], [41.822679908, -87.612512046], [41.773892091, -87.58630763]]]
    for liP in range(len(starting_centroids)):
        start_time = time.time()
        wrCentroid.create_file(liP, starting_centroids[liP])
        name = "start_c_" + str(liP) + ".txt"
        

        kmeans = spark_kmeans()


        distFile = sc.textFile("data.txt")
        
        rdd = spark.read.csv('hdfs://namenode:9000/DAT500/spark_preprocess.csv', schema=crimes_schema)
        points = rdd.select("Location")

        centroids = wrCentroid.retrieveCentroids(name)
        # centroids = wrCentroid.retrieveCentroids('starting_centroids.txt')
        
        # first run
        pointList = wrCentroid.convertPoints(points)
        mapped_points = kmeans.map(pointList, centroids)
        new_centroids = kmeans.reduce(mapped_points)
        
        iterations = 1
        start_time = time.time()
        # after first run
        while True:
            min_dist = 0.001
            done = True
            for i in range(len(new_centroids)):
                distance = math.sqrt(pow(centroids[i][0]-new_centroids[i][0], 2) + pow(centroids[i][1] - new_centroids[i][1], 2)) 
                if distance > min_dist:
                    done = False

            iterations += 1
            if done:
                end_time = time.time()
                run_time = end_time - start_time
                time_list = [start_time, end_time, run_time]
                # wrCentroid.writeTime(time_list)
                wrCentroid.createTimeFile(liP, time_list)
                print(new_centroids)
                print("Iterations: %s" % iterations)
                print("--- %s seconds ---" % (time.time() - start_time))
                break
            else:
                centroids = new_centroids
                mapped_points = kmeans.map(pointList, centroids)
                new_centroids = kmeans.reduce(mapped_points)

    # """
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
