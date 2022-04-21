import os
from typing import Counter
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
# os.environ["SPARK_HOME"] = "/content/spark-2.4.5-bin-hadoop2.7"

import findspark
from matplotlib import pyplot as plt
import numpy as np
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# spark imports
from pyspark.sql import SparkSession
import csv
from pyspark.sql.types import *

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
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
# dataset.head(5)

# primaryCount = dataset.groupby('Primary Type').count()
# primaryCount.orderBy('count', ascending=False).show()

location = dataset.limit(100000).select("Location")


##############################################################################################################
###################################### this is with longitude/latitude #######################################
##############################################################################################################

# Latitude = dataset.limit(1000).select("Latitude")
# Longitude = dataset.limit(1000).select("Longitude")

# '''
# Converts location data from str to float
# '''
# col_header = ["Latitude", "Longitude"]
# coords_dataset = Latitude.join(Longitude)
# df = coords_dataset.withColumn("id", monotonically_increasing_id())

# '''
# Create a features column to be used in the clustering
# '''
# vecAssembler = VectorAssembler(inputCols=col_header, outputCol="features")
# df_kmeans = vecAssembler.transform(df).select('id', 'features')
# # print("Show df_means: ")
# # df_kmeans.show()

# '''
# Train the machine learning model using the KMeans mllib function
# '''
# k = 5
# kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
# model = kmeans.fit(df_kmeans)
# centers = model.clusterCenters()

# x_list = []
# y_list = []
# #print("Cluster Centers: ")
# for center in centers:
#     x_list.append(center[0])
#     y_list.append(center[1])
#     #print(center)

    
# '''
# Assign clusters to events
# '''
# transformed = model.transform(df_kmeans).select('id', 'prediction')
# rows = transformed.collect()


# df_pred = spark.createDataFrame(rows)
# #df_pred.show()

# '''
# Join the prediction with the original data
# Then convert the data over to Pandas dataframe
# '''
# df_pred = df_pred.join(df, 'id')
# #df_pred.show()

# pddf_pred = df_pred.toPandas().set_index('id')
# pddf_pred.head()

# '''
# Visualize the result
# '''
# plt.scatter(pddf_pred.Latitude, pddf_pred.Longitude, c=pddf_pred.prediction)
# plt.scatter(x_list, y_list, c='red')
# plt.xlabel('x')
# plt.ylabel('y')
# plt.show()

##############################################################################################################
############################ made this to a class instead (see below this) ###################################
##############################################################################################################

# col_header = ["X_coord", "Y_coord"]
# allcoord = []
# for coord in location.collect():
#     cleaned_coord = ''.join(c for c in coord["Location"] if c not in '()')
#     new_coords = cleaned_coord.split(',')
#     x_coord = float(new_coords[1])
#     y_coord = float(new_coords[0])
#     coords = [x_coord, y_coord]
#     allcoord.append(coords)
# df = spark.createDataFrame(data=allcoord, schema = col_header)
# df = df.withColumn("id", monotonically_increasing_id())

# '''
# Create a features column to be used in the clustering
# '''
# vecAssembler = VectorAssembler(inputCols=col_header, outputCol="features")
# df_kmeans = vecAssembler.transform(df).select('id', 'features')
# #print("Show df_means: ")
# #df_kmeans.show()


# '''
# Train the machine learning model using the KMeans mllib function
# '''
# k = 5
# kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
# model = kmeans.fit(df_kmeans)
# centers = model.clusterCenters()

# x_list = []
# y_list = []
# print("Cluster Centers: ")
# for center in centers:
#     x_list.append(center[0])
#     y_list.append(center[1])
#     print(center)

    
# '''
# Assign clusters to events
# '''
# transformed = model.transform(df_kmeans).select('id', 'prediction')
# rows = transformed.collect()


# df_pred = spark.createDataFrame(rows)
# #df_pred.show()

# '''
# Join the prediction with the original data
# Then convert the data over to Pandas dataframe
# '''
# df_pred = df_pred.join(df, 'id')
# #df_pred.show()

# pddf_pred = df_pred.toPandas().set_index('id')
# pddf_pred.head()

# '''
# Visualize the result
# '''
# plt.scatter(pddf_pred.X_coord, pddf_pred.Y_coord, c=pddf_pred.prediction)
# plt.scatter(x_list, y_list, c='red')
# plt.xlabel('x')
# plt.ylabel('y')
# plt.show()

##############################################################################################################
##############################################################################################################
##############################################################################################################

class spark_k_means():
    def __init__(self):
        self.allcoord = []
        self.col_header = ["X_coord", "Y_coord"]
        self.pddf_pred = None
        self.x_list = []
        self.y_list = []
        self.df = None
    
    def test(self):
        for coord in location.collect():
            cleaned_coord = ''.join(c for c in coord["Location"] if c not in '()')
            new_coords = cleaned_coord.split(',')
            x_coord = float(new_coords[1])
            y_coord = float(new_coords[0])
            coords = [x_coord, y_coord]
            self.allcoord.append(coords)
        self.df = spark.createDataFrame(data=self.allcoord, schema = self.col_header)
        self.df = self.df.withColumn("id", monotonically_increasing_id())
        
    def create_k_means(self):
        vecAssembler = VectorAssembler(inputCols=self.col_header, outputCol="features")
        df_kmeans = vecAssembler.transform(self.df).select('id', 'features')
        k = 5
        kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(df_kmeans)
        centers = model.clusterCenters()


        for center in centers:
            self.x_list.append(center[0])
            self.y_list.append(center[1]) 
            print(center)  
            
        transformed = model.transform(df_kmeans).select('id', 'prediction')
        rows = transformed.collect()
        
        df_pred = spark.createDataFrame(rows)
        df_pred = df_pred.join(self.df, 'id')
        
        self.pddf_pred = df_pred.toPandas().set_index('id')
    
    def plot(self):
        plt.scatter(self.pddf_pred.X_coord, self.pddf_pred.Y_coord, c=self.pddf_pred.prediction)
        plt.scatter(self.x_list, self.y_list, c='red')
        plt.xlabel('x')
        plt.ylabel('y')
        plt.show()
        
    def fit(self):
        self.test()
        self.create_k_means()
        
if __name__ == "__main__":
    kmeans = spark_k_means()
    kmeans.fit()
    kmeans.plot()