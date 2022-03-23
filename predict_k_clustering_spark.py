import os
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


# Date;Block;Primary Type;Description;Arrest;Domestic;Beat;Community Area;Year;Updated On;Location
crimes_schema = StructType([# StructField("ID", IntegerType(), True),
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

data = spark.read.csv('2_successfull_preprocesses.csv', header = True,schema = crimes_schema)
data.head(5)

primaryCount = data.groupby('Primary Type').count()
primaryCount.orderBy('count', ascending=False).show()

