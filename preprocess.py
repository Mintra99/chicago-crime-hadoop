from distutils.log import error
from mpl_toolkits.mplot3d import Axes3D
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt # plotting
import numpy as np # linear algebra
import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import csv
from datetime import *

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
        self.datetime_convert()

    def set_dataframes(self):
        nRowsRead = 1000
        self.CC_0104 = pd.read_csv("data/Chicago_Crimes_2001_to_2004.csv", delimiter=',', nrows = nRowsRead)
        self.CC_0507 = pd.read_csv("data/Chicago_Crimes_2005_to_2007.csv", delimiter=',', nrows = nRowsRead)
        self.CC_0811 = pd.read_csv("data/Chicago_Crimes_2008_to_2011.csv", delimiter=',', nrows = nRowsRead)
        self.CC_1217 = pd.read_csv("data/Chicago_Crimes_2012_to_2017.csv", delimiter=',', nrows = nRowsRead)
        
    def datetime_convert(self):
        self.CC_0104["Date"] = pd.to_datetime(self.CC_0104['Date'], format='%m/%d/%Y %I:%M:%S %p').dt.strftime('%d/%m/%Y %H:%M:%S')
        self.CC_0507["Date"] = pd.to_datetime(self.CC_0507['Date'], format='%m/%d/%Y %I:%M:%S %p').dt.strftime('%d/%m/%Y %H:%M:%S')
        self.CC_0811["Date"] = pd.to_datetime(self.CC_0811['Date'], format='%m/%d/%Y %I:%M:%S %p').dt.strftime('%d/%m/%Y %H:%M:%S')
        self.CC_1217["Date"] = pd.to_datetime(self.CC_1217['Date'], format='%m/%d/%Y %I:%M:%S %p').dt.strftime('%d/%m/%Y %H:%M:%S')

    def set_columns(self):
        self.columns = self.CC_0104.columns
    
    def get_columns(self):
        return self.columns
    
    def get_rows_by_year(self):
        print("Please select a year between 2001 and 2017")
        year = input("Enter what year you want: ")
        if year >= 2001 and year <= 2004:
            return self.CC_0104.loc[self.CC_0104["Year"] == year]
        if year > 2004 and year <= 2007:
            return self.CC_0507.loc[self.CC_0104["Year"] == year]
        if year > 2007 and year <= 2011:
            return self.CC_0811.loc[self.CC_0104["Year"] == year]
        if year > 2011 and year <= 2017:
            return self.CC_1217.loc[self.CC_0104["Year"] == year]
        else:
            return "error: out of bounds"
        
    def get_rows_by_crime(self):
        # Just doing this to make it easier to see what kind of crime we can choose between
        print("Choose between:\nTHEFT\nOTHER OFFENSE\nOFFENSE INVOLVING CHILDREN")
        print("CRIM SEXUAL ASSAULT\nMOTOR VEHICLE THEFT\nSEX OFFENSE")
        print("DECEPTIVE PRACTICE\nBATTERY\nBURGLARY")
        print("WEAPONS VIOLATION\nPUBLIC PEACE VIOLATION\nNARCOTICS")
        print("GAMBLING\nPROSTITUTION\nLIQUOR LAW VIOLATION")
        print("INTERFERENCE WITH PUBLIC OFFICER\nCRIMINAL DAMAGE\nASSAULT")
        print("STALKING\nARSON\nCRIMINAL TRESPASS")
        crime = input("Enter what crime you want: ")
        
        rows = []
        rows.append(self.CC_0104.loc[self.CC_0104['Primary Type'] == crime])
        rows.append(self.CC_0507.loc[self.CC_0507['Primary Type'] == crime])
        rows.append(self.CC_0811.loc[self.CC_0811['Primary Type'] == crime])
        rows.append(self.CC_1217.loc[self.CC_1217['Primary Type'] == crime])
        return rows
    
    
    
test = preprocess()
test.fit()
print(test.get_columns())
print(test.get_rows_by_year())
print(test.get_rows_by_crime())
