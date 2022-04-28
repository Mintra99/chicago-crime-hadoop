# run.py
import math
from re import L
# from utkast_mapReduce import KMeansJob

# import argparse
# import matplotlib.pyplot as plt
# import numpy as np
# import pandas as pd

# import os
import subprocess
import time
# from tkinter.tix import Tree
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
    
    def writeTime(self, time_list):
        f = open('time_list_file.txt', "w+")
        string_item = 'start: ' + str(time_list[0]) + ' | end: ' + str(time_list[1]) + ' | run time: ' + str(time_list[2])
        f.write("%s\n" % string_item)
        f.close()
    
    def create_file(self, centroids, name):
        print(name)
        f = open(name, "w")
        iteration = 1
        for item in centroids:
            string_item = str(iteration) + ' ; ' + str(item) + ' ; 1'
            f.write("%s\n" % string_item)
        f.close()
    
    def createTimeFile(self, time_list, start_c, version, iterations, file):
        name = "TIME_LIST_" + str(file)
        f = open(name, "w")
        string_item = 'start: ' + str(time_list[0]) + ' | end: ' + str(time_list[1]) \
        + ' | run time: ' + str(time_list[2]) + ' | start_coord: ' + str(start_c) \
        + " | version: " + str(version) + " | iterations: " + str(iterations)
 
        
        f.write("%s\n" % string_item)
        f.close()


# file = "starting_centroids.txt"
output = 'final_centroids.txt'


if __name__ == "__main__":
    wrCentroid = WRCentroids()
    
    wrCentroid = WRCentroids()
    starting_centroids = [[[41.812842218, -87.728659989], [41.909408388, -87.675949324], [41.705169694, -87.63708421], [41.898916021, -87.732333607], [41.744235532, -87.551407988]],
    [[41.899082422, -87.71917838], [41.941161268, -87.642667917], [41.960447836, -87.669222376], [41.883224344, -87.624971297], [41.852589811, -87.713647735]],
    [[41.910008891, -87.715396172], [41.93688984, -87.721778097], [41.747672195, -87.601090224], [41.848229383, -87.633428407], [41.77514011, -87.590017895]],
    [[41.760735939, -87.647937849], [41.742372451, -87.637668133], [41.707250544, -87.604006449], [41.924672785, -87.711094988], [41.930414054, -87.762393371]]]
    version_list = ["utkast_fileVersion_MI.py",
    "utkast_fileVersion_MRI.py", "utkast_redV2_fileVersion_MI.py",
    "utkast_fileVersion.py", "utkast_fileComposer.py",
    "utkast_fileComposer_init.py"]
    for liP in range(2):
        start_c = starting_centroids[liP]

        for v in range(len(version_list)):
            version = version_list[v]
            file = "HADOOP_" + str(version) + str(liP) + ".txt"
            output = "HADOOP_FINAL_" + str(version) + str(liP) + ".txt"
            wrCentroid.create_file(start_c, file)

            iterations = 1
            start_time = time.time()
            while True:
                command = "python3 " + str(version) + " < " \
                + "python_preprocess.csv " + " --centroids " + file + " > " + output \
                + " -r hadoop"

                p = subprocess.Popen(command, shell=True)
                p.communicate()

                new_centroids = wrCentroid.retrieveCentroids(output)

                min_dist = 0.001
                done = True
                for i in range(len(new_centroids)):
                    distance = math.sqrt(pow(centroids[i][0]-new_centroids[i][0], 2) + pow(centroids[i][1] - new_centroids[i][1], 2)) 
                    if distance > min_dist:
                        done = False
                
                if done:
                    end_time = time.time()
                    run_time = end_time - start_time
                    time_list = [start_time, end_time, run_time]
                    wrCentroid.createTimeFile(liP, time_list, liP)
                    break
                else:
                    centroids = new_centroids
                    if len(centroids) != 0:
                        wrCentroid.writeCentroids(centroids, file)
                
                iterations +=1
            # """
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