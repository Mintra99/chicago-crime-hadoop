import os
from typing import Counter
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
# os.environ["SPARK_HOME"] = "/content/spark-2.4.5-bin-hadoop2.7"

import findspark
from matplotlib import pyplot as plt
import numpy as np
from sklearn.preprocessing import MinMaxScaler
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# spark imports
from pyspark import SQLContext, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import csv
from pyspark.sql.types import *
from pyspark.sql.functions import format_number, when
import pyspark.sql.functions as F

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import monotonically_increasing_id


# Date;Block;Primary Type;Description;Arrest;Domestic;Beat;Community Area;Year;Updated On;Location
crimes_schema = StructType([StructField("ID", IntegerType(), True),
                            # StructField("FBI Code", StringType(), True ),
                            StructField("Date", StringType(), True ),
                            StructField("Block", StringType(), True),
                            StructField("Primary Type", StringType(), True  ),
                            StructField("Description", StringType(), True ),
                            # StructField("Location Description", StringType(), True ),
                            StructField("Arrest", BooleanType(), True),
                            StructField("Domestic", BooleanType(), True),
                            StructField("Beat", IntegerType(), True),
                            # StructField("District", IntegerType(), True),
                            # StructField("Ward", IntegerType(), True),
                            StructField("Community Area", IntegerType(), True),
                            # StructField("X Coordinate", IntegerType(), True),
                            # StructField("Y Coordinate", IntegerType(), True ),
                            StructField("Year", IntegerType(), True),
                            StructField("Updated On", StringType(), True ),
                            # StructField("Latitude", DoubleType(), True),
                            # StructField("Longitude", DoubleType(), True),
                            StructField("Location", StringType(), True )
                            ])

dataset = spark.read.csv('python_preprocess.csv', header = True,schema = crimes_schema)
dataset = dataset.drop("ID")
# dataset.head(5)

# primaryCount = dataset.groupby('Primary Type').count()
# primaryCount.orderBy('count', ascending=False).show()

location = dataset.limit(100000).select("Location")
crime = dataset.limit(100000).select("Primary Type")
crime_dict = {}
for c in crime.collect():
    if c not in crime_dict:
        crime_dict[c] = len(crime_dict)
        
test = []
for cr in crime.collect():
    test.append(crime_dict[cr])
'''
Converts location data from str to float
'''
allcoord = []
col_header = ["X_coord", "Y_coord"]
for coord in location.collect():
    cleaned_coord = ''.join(c for c in coord["Location"] if c not in '()')
    new_coords = cleaned_coord.split(',')
    x_coord = float(new_coords[1])
    y_coord = float(new_coords[0])
    coords = [x_coord, y_coord]
    allcoord.append(coords)
df = spark.createDataFrame(data=allcoord, schema = col_header)
df = df.withColumn("id", monotonically_increasing_id())

'''
Create a features column to be used in the clustering
'''
vecAssembler = VectorAssembler(inputCols=col_header, outputCol="features")
df_kmeans = vecAssembler.transform(df).select('id', 'features')
#print("Show df_means: ")
#df_kmeans.show()


'''
Train the machine learning model using the KMeans mllib function
'''
k = 5
kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(df_kmeans)
centers = model.clusterCenters()

x_list = []
y_list = []
for center in centers:
    x_list.append(center[0])
    y_list.append(center[1])


    
'''
Assign clusters to events
'''
transformed = model.transform(df_kmeans).select('id', 'prediction')
rows = transformed.collect()


df_pred = spark.createDataFrame(rows)
#df_pred.show()
df.show()

'''
Join the prediction with the original data
Then convert the data over to Pandas dataframe
'''
df_pred = df_pred.join(df, 'id')
#df_pred.show()

pddf_pred = df_pred.toPandas().set_index('id')
pddf_pred.head()

'''
Visualize the result
'''
plt.scatter(pddf_pred.X_coord, pddf_pred.Y_coord, c=test)
plt.xlabel('x')
plt.ylabel('y')
plt.show()