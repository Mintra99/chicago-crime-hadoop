import math
import os
from typing import Counter
from unittest import skip
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
# os.environ["SPARK_HOME"] = "/content/spark-2.4.5-bin-hadoop2.7"

import sys
sys.setrecursionlimit(1000)

import findspark
from matplotlib import pyplot as plt
import numpy as np

findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# spark imports
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id


# Date;Block;Primary Type;Description;Arrest;Domestic;Beat;Community Area;Year;Updated On;Location
crimes_schema = StructType([StructField("ID", IntegerType(), True),
                            # StructField("FBI Code", StringType(), True ),
                            StructField("Date", StringType(), True ),
                            # StructField("Block", StringType(), True),
                            StructField("Primary Type", StringType(), True  ),
                            # StructField("Description", StringType(), True ),
                            # StructField("Location Description", StringType(), True ),
                            # StructField("Arrest", BooleanType(), True),
                            # StructField("Domestic", BooleanType(), True),
                            # StructField("Beat", IntegerType(), True),
                            # StructField("District", IntegerType(), True),
                            # StructField("Ward", IntegerType(), True),
                            # StructField("Community Area", IntegerType(), True),
                            # 
                            # StructField("X Coordinate", IntegerType(), True),
                            # StructField("Y Coordinate", IntegerType(), True ),
                            # StructField("Year", IntegerType(), True),
                            # StructField("Updated On", StringType(), True ),
                            StructField("Latitude", DoubleType(), True),
                            StructField("Longitude", DoubleType(), True),
                            StructField("Location", StringType(), True )
                            ])


dataset = spark.read.csv('python_preprocess.csv', header = True,schema = crimes_schema)
dataset = dataset.drop("ID")

location = dataset.limit(150000).select("Location")
#location = dataset.select("Location")

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
        
        self.X = []
        self.Y = []
        
    def fit(self):
        self.convert_coords()
        self.create_coord_df()
        self.kmeans()

        
    def convert_coords(self):
        for coord in location.collect():
            cleaned_coord = ''.join(c for c in coord["Location"] if c not in '()')
            new_coords = cleaned_coord.split(',')
            x_coord = float(new_coords[0])
            y_coord = float(new_coords[1])
            coords = [x_coord, y_coord]
            self.coords.append(coords)
    
    def create_coord_df(self):
        col_header = ["X_coord", "Y_coord"]            
        df = spark.createDataFrame(data=self.coords, schema = col_header)
        self.coord_df = df.withColumn("id", monotonically_increasing_id())
        for point in self.coords:
            self.X.append(point[0])
            self.Y.append(point[1])
    
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
        
    def plot(self):
        plt.scatter(self.X, self.Y, c=self.prediction)
        plt.scatter(self.x_list, self.y_list, c='red')
        plt.xlabel('x')
        plt.ylabel('y')
        plt.show()
            
if __name__ == "__main__":
    kmeans = spark_kmeans()
    kmeans.fit()
    kmeans.plot()