import math
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
    def __init__(self):
        self.coords = []
        self.x_list = []
        self.y_list = []
        self.prediction = []
        self.coord_df = None
        self.closest_centroid = {}
        self.prev_centroid = []
        self.centroids = [[41.775185697, -87.659244248],
                           [41.926404101, -87.792881805],
                           [41.846664648, -87.617318718],
                           [41.954345702, -87.726412567]]
        
        
    def fit(self):
        self.convert_coords()
        self.kmeans()

        
    def convert_coords(self):
        for coord in location.collect():
            cleaned_coord = ''.join(c for c in coord["Location"] if c not in '()')
            new_coords = cleaned_coord.split(',')
            x_coord = float(new_coords[0])
            y_coord = float(new_coords[1])
            coords = [x_coord, y_coord]
            self.coords.append(coords)
    
    def getDistance(self, centroid, point):
        cur_dist = math.sqrt((centroid[0]-point[0])**2 + (centroid[0]-point[0])**2) 
        return cur_dist    
        
    def map(self):
        self.closest_centroid = {}
        self.prediction = []
        for point in self.coords:
            min_dist = math.inf
            closest_centroid = -1
            for centroid in self.centroids:
                curr_dist = self.getDistance(centroid, point)                
                if curr_dist < min_dist:
                    min_dist = curr_dist
                    closest_centroid = centroid
                    
            self.prediction.append(self.centroids.index(closest_centroid))
            if tuple(closest_centroid) not in self.closest_centroid:
                self.closest_centroid[tuple(closest_centroid)] = list()
            self.closest_centroid[tuple(closest_centroid)].extend([point])
 
    def reduce(self):
        new_centroid = []
        for centroid in self.closest_centroid:
            X = []
            Y = []
            counter=0
            for point in self.closest_centroid[centroid]:
                X.append(point[0])
                Y.append(point[1])
                counter += 1
            new_centroid.append([sum(X)/counter, sum(Y)/counter])
            
        self.prev_centroid = self.centroids
        self.centroids = new_centroid
    
    def kmeans(self):
        while self.centroids != self.prev_centroid:
            self.map()   
            self.reduce()  
            
        for centroid in self.centroids:
            self.x_list.append(centroid[0])
            self.y_list.append(centroid[1])
            print(centroid)


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


if __name__ == "__main__":
    """install_requires=[
        'pyspark=={site.SPARK_VERSION}'
    ]"""

    wrCentroid = WRCentroids()

    conf = SparkConf().setAppName("PySPark CrimesChicago").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    #findspark.init()
    """
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    dataset = spark.read.csv('spark_preprocess.csv', header = True,schema = crimes_schema)
    dataset = dataset.drop("ID")
    location = dataset.limit(150000).select("Location")
    """

    rdd = sc.textFile("hdfs://DAT500/spark_preprocess.csv")

    # lambda = rows/lines
    # mapper lengden til linja.
    # reducer alt
    rdd.map(lambda s: len(s)).reduce(lambda a, b: a + b)

    i = 1 # + " --centroids=" \ # mellom data og files


    # distFile.map(lambda s: len(s)).reduce(lambda a, b: a + b)


    """
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
    """

    kmeans = spark_kmeans()
    kmeans.fit()

    # https://spark.apache.org/docs/latest/rdd-programming-guide.html
