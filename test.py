from mpl_toolkits.mplot3d import Axes3D
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt # plotting
import numpy as np # linear algebra
import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import csv

class preprocess():
    def __init__(self):
        self.CC_0104 = None
        self.CC_0507 = None
        self.CC_0811 = None
        self.CC_1217 = None
        self.columns = None
        
    def fit(self):
        self.set_dataframes()
        self.set_columns()

    
    def set_dataframes(self):
        nRowsRead = 1000
        self.CC_0104 = pd.read_csv("data/Chicago_Crimes_2001_to_2004.csv", delimiter=',', nrows = nRowsRead)
        self.CC_0104 = pd.read_csv("data/Chicago_Crimes_2005_to_2007.csv", delimiter=',', nrows = nRowsRead)
        self.CC_0104 = pd.read_csv("data/Chicago_Crimes_2008_to_2011.csv", delimiter=',', nrows = nRowsRead)
        self.CC_0104 = pd.read_csv("data/Chicago_Crimes_2012_to_2017.csv", delimiter=',', nrows = nRowsRead)

    def set_columns(self):
        self.columns = self.CC_0104.columns
    
    def get_columns(self):
        return self.columns
    
    
    
test = preprocess()
test.fit()
print(test.get_columns())
