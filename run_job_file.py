# run.py
import math
from re import L
# from utkast_mapReduce import KMeansJob

import argparse
# import matplotlib.pyplot as plt
# import numpy as np
# import pandas as pd

import os
import subprocess
# import sys

# python3 run_job.py < python_preprocess.csv > file

# Kopier det inn i en fil
# les av den
# Pass navnet til files
# --file

class WRCentroids():
    """
    def initialCentroids(self, file, nclusters):
        initial_centroids = [[41.75722769555852, -87.64231203969189],
        [41.90692766400154, -87.76994270927075],
        [41.86249005695598, -87.6402480122286],
        [41.934092855429704, -87.70006184850016]]
        return initial_centroids
    """
    
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


# file = "starting_centroids.txt"
output = 'final_centroids.txt'

if __name__ == "__main__":
    wrCentroid = WRCentroids()
    
    """
    dim = [[41.98131263, -87.806945473],
    [41.771488695, -87.667641182],
    [41.884494554, -87.627138636],
    [41.754594962, -87.70872738],
    [41.840581183804865, -87.67204270608761]]
    wrCentroid.writeCentroids(dim, file)
    """
    # wrCentroid.badWrite(dim, 'old_type_centroid.txt')

    # parser = argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--inputFile", type=str,
    help="python_preprocess.csv")

    parser.add_argument("--centroids", type=str,
    help="starting_centroids.txt")

    args = parser.parse_args()
    data = args.inputFile # 'python_preprocessed.csv'
    file = args.centroids
    # if not exist
    outputFile = open(output, "w+")
    outputFile.close()

    centroids = wrCentroid.retrieveCentroids(file)

    i = 1 # + " --centroids=" \ # mellom data og files
    while True:
        print("ITERATION: " + str(i))
        command = "python3 utkast_fileVersion.py < " \
        + data + " --centroids " + file + " > " + output \
        + " -r inline"
        # if in local
        # if in hadoop
        print(command)

        # proc = os.popen(command)
        # print(str(proc))
        # python3 utkast_fileVersion.py <
        # python_preprocess.csv --centroids
        # starting_centroids.txt >
        # final_centroids.txt -r inline
        


        p = subprocess.Popen(command, shell=True)
        p.communicate() #now wait plus that you can send commands to process
        # print(p)

        new_centroids = wrCentroid.retrieveCentroids(output)
        print("NEW CENTROIDS: " + str(new_centroids))

        # """
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
        # """
        
        i +=1
        # break
        
    
    # mapper is provided data from hdfs
    # os.remove(output)
    """
        "python hadoop.py < data + starting_cemtroids > + output -r hadoop"
    """





"""
# while 1 < 2:
for i in range(2):
    if i == 0:
        old_dim = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
    mr_job = KMeansJob(args=['-r', 'hadoop', '--jobconf', 'my.job.settings.starting_values='+ str(old_dim)])
    with mr_job.make_runner() as runner:
        # '--conf-path', 'mrjob.conf',
        new_dim = [0, 0, 0, 0]
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            new_dim[key] = value
        if new_dim == old_dim:
            p("DONE")
            p(new_dim)
            break
        old_dim = new_dim
        p(new_dim)
"""

    # you can read external files in the mapper

    # ... etc