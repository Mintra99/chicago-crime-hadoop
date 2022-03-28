import os
from typing import Counter
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
# os.environ["SPARK_HOME"] = "/content/spark-2.4.5-bin-hadoop2.7"

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# spark imports
from pyspark import SparkContext
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
                            # 
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

location = dataset.limit(10).select("Location")

#location = location.select(*(location[c].cast("float").alias(c) for c in location.columns[:]))
allcoord = []
col_header = ["X_coord", "Y_coord"]
for coord in location.collect():
    cleaned_coord = ''.join(c for c in coord["Location"] if c not in '()')
    new_coords = cleaned_coord.split(',')
    x_coord = float(new_coords[0])
    y_coord = float(new_coords[1])
    coords = [x_coord, y_coord]
    allcoord.append(coords)
coord_df = spark.createDataFrame(data=allcoord, schema = col_header)
coord_df = coord_df.withColumn("id", monotonically_increasing_id())
coord_df.show(20, False)

# for col in coord_df.columns:
#     if col in col_header:
#         coord_df = coord_df.withColumn(col,coord_df[col].cast('float'))
# coord_df.show()

vecAssembler = VectorAssembler(inputCols=col_header, outputCol="features")
df_kmeans = vecAssembler.transform(coord_df).select('id', 'features')

k = 3
kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(df_kmeans)
centers = model.clusterCenters()

print("Cluster Centers: ")
for center in centers:
    print(center)