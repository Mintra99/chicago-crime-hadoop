import pandas as pd
import numpy as np

# set dataframes
# import data
nRowsRead = 100
CC_0104 = pd.read_csv("data/Chicago_Crimes_2001_to_2004.csv", delimiter=',', nrows = nRowsRead)
CC_0507 = pd.read_csv("data/Chicago_Crimes_2005_to_2007.csv", delimiter=',', nrows = nRowsRead)
CC_0811 = pd.read_csv("data/Chicago_Crimes_2008_to_2011.csv", delimiter=',', nrows = nRowsRead)
CC_1217 = pd.read_csv("data/Chicago_Crimes_2012_to_2017.csv", delimiter=',', nrows = nRowsRead)

# put it into the form we desir
dataA = [CC_0104, CC_0507, CC_0811, CC_1217]
datalist = []
for i in dataA:
    for j in i.values:
        datalist.append(j)

# print(datalist)

# Make the data a panda dataframe
data = pd.DataFrame(datalist)

# Remove duplicates
data.drop_duplicates()

new_col = CC_0104.columns.tolist()
data.columns = new_col

drop_list = ["NUM", "ID", "Case Number", "IUCR", "FBI Code", "Latitude", "Longitude", "Location Description", "Ward", "District", "X Coordinate", "Y Coordinate"]
# drop_list = ["FBI Code", "Latitude", "Longitude", "Location Description", "Ward", "District", "X Coordinate", "Y Coordinate"]
data.drop(drop_list, axis = 1, inplace = True)

# removing nullvalues
# data[['X Coordinate', 'Y Coordinate']] = data[['X Coordinate', 'Y Coordinate']].replace(0, np.nan)
# print(data[['Location']])

# data[['Location']] = data[['Location']].replace("", np.nan)
"""
for i in drop_list:
    data[['Community Area']] = data[['Community Area']].replace('', np.nan)
    data.dropna(subset=['Location'], inplace=True)
"""
data[['Location']] = data[['Location']].replace('', np.nan)
data[['Community Area']] = data[['Community Area']].replace('', np.nan)
data.dropna(subset=['Location'], inplace=True)
data.dropna(subset=['Community Area'], inplace=True)
# data=data.na.drop()
data.drop_duplicates()
data.to_csv('python_preprocess.csv', sep=';', index=False)


# removing irrelevant columns (Just add columns in drop_list to remove them)


# data.to_csv('Sampe2_verySmall.csv', index=False)