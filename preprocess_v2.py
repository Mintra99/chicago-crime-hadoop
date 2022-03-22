import functools
from matplotlib.pyplot import show
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp, col
import plotly.express as px
import matplotlib.pyplot as plt # plotting
spark = SparkSession.builder.appName("Chicago_crime_analysis").getOrCreate()
from pyspark.sql.types import  (StructType, 
                                StructField, 
                                DateType, 
                                BooleanType,
                                DoubleType,
                                IntegerType,
                                StringType,
                               TimestampType)

class preprocess():
    def __init__(self):
        self.CC_0104 = None
        self.CC_0507 = None
        self.CC_0811 = None
        self.CC_1217 = None
        self.to_present = None
        
    def fit(self):
        self.set_dataframes()
        self.__datetime_converter()
        
    def __unionAll(self, dfs):
        return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

    def set_dataframes(self):
        crimes_schema = StructType([
                            StructField("Unnamed: 0", StringType(),True),
                            StructField("ID", StringType(), True),
                            StructField("CaseNumber", StringType(), True),
                            StructField("Date", StringType(), True ),
                            StructField("Block", StringType(), True),
                            StructField("IUCR", StringType(), True),
                            StructField("PrimaryType", StringType(), True  ),
                            StructField("Description", StringType(), True ),
                            StructField("LocationDescription", StringType(), True ),
                            StructField("Arrest", BooleanType(), True),
                            StructField("Domestic", BooleanType(), True),
                            StructField("Beat", StringType(), True),
                            StructField("District", StringType(), True),
                            StructField("Ward", StringType(), True),
                            StructField("CommunityArea", StringType(), True),
                            StructField("FBICode", StringType(), True ),
                            StructField("XCoordinate", DoubleType(), True),
                            StructField("YCoordinate", DoubleType(), True ),
                            StructField("Year", IntegerType(), True),
                            StructField("UpdatedOn", DateType(), True ),
                            StructField("Latitude", DoubleType(), True),
                            StructField("Longitude", DoubleType(), True),
                            StructField("Location", StringType(), True )
                            ])
        
        self.CC_0104 = spark.read.csv("data/Chicago_Crimes_2001_to_2004.csv", header = True, schema=crimes_schema)
        self.CC_0507 = spark.read.csv("data/Chicago_Crimes_2005_to_2007.csv", header = True, schema=crimes_schema)
        self.CC_0811 = spark.read.csv("data/Chicago_Crimes_2008_to_2011.csv", header = True, schema=crimes_schema)
        self.CC_1217 = spark.read.csv("data/Chicago_Crimes_2012_to_2017.csv", header = True, schema=crimes_schema)
        
        self.to_present = self.__unionAll([self.CC_0104, self.CC_0507, self.CC_0811, self.CC_1217])
        self.to_present = self.to_present.where(col("XCoordinate").isNotNull())
        self.to_present = self.to_present.where(col("Ward").isNotNull())
        self.to_present = self.to_present.where(col("CommunityArea").isNotNull())
        self.to_present = self.to_present.drop("Unnamed: 0","ID", "CaseNumber", "IUCR", "FBICode", "Location", 
                                               "District", "CommunityArea", "UpdatedOn","XCoordinate", 
                                               "YCoordinate", "Domestic", "Beat", "Description", "Block")
        self.to_present = self.to_present.na.drop()
        
        
        
        
    def __datetime_converter(self):
        self.CC_0104 = self.CC_0104.withColumn('Date', from_unixtime(unix_timestamp(col(('Date')), "MM/dd/yyyy hh:mm:ss a"), "yyyy-MM-dd HH:mm:ss"))
        self.CC_0507 = self.CC_0507.withColumn('Date', from_unixtime(unix_timestamp(col(('Date')), "MM/dd/yyyy hh:mm:ss a"), "yyyy-MM-dd HH:mm:ss"))
        self.CC_0811 = self.CC_0811.withColumn('Date', from_unixtime(unix_timestamp(col(('Date')), "MM/dd/yyyy hh:mm:ss a"), "yyyy-MM-dd HH:mm:ss"))
        self.CC_1217 = self.CC_1217.withColumn('Date', from_unixtime(unix_timestamp(col(('Date')), "MM/dd/yyyy hh:mm:ss a"), "yyyy-MM-dd HH:mm:ss"))
        self.to_present = self.to_present.withColumn('Date', from_unixtime(unix_timestamp(col(('Date')), "MM/dd/yyyy hh:mm:ss a"), "yyyy-MM-dd HH:mm:ss"))    
            
            
    def get_data(self):
        #return self.to_present.select("Date").show(20, False)
        return self.to_present.show(20, False)
    
    def map_plotter(self):
        # Have limited it to 1000 becaus using the whole data crashees the spark session
        # Edit it so that we have more specific stuf to find
        map_marks = self.to_present.limit(1000).toPandas()
        map_marks[['Latitude', 'Longitude']]
        fig = px.scatter_mapbox(map_marks, lat="Latitude", lon="Longitude", 
                        color_discrete_sequence=["fuchsia"], zoom=9, height=650)
        fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        fig.show()
        
    # Ideas for things to do
    # https://medium.com/@stafa002/my-notes-on-chicago-crime-data-analysis-ed66915dbb20
    # https://www.tandfonline.com/doi/abs/10.1080/02522667.2019.1582878

    def piechart(self):
        plt.style.use(plt.style.available[9])
        plt.figure(figsize=(8,8))
        plt.pie(
            self.to_present.limit(100000).toPandas().groupby("PrimaryType").sum()["Arrest"],
            labels=self.to_present.limit(100000).toPandas().groupby("PrimaryType").sum().index,
            radius=1, 
            autopct='%0.1f%%', )
            #explode=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])
        plt.title('Crime types', fontdict={'fontsize': 16})
        plt.show()
    
    
test = preprocess()
test.fit()
#test.get_data()
#test.map_plotter()
test.piechart()

