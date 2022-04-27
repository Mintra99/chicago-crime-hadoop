from mrjob.job import MRJob
# from k_clustering_point_class import Cluster
# from mrjob.step import MRStep
import math
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.compat import jobconf_from_env
import sys

"""
output = Kmeansjob(original_dimensions)
while loop
    new_output = Kmeansjob(output)
    if new_output == output:
        break
    output = new_output
"""

"""
For example, a ~mrjob.job.MRJob could use
jobconf_from_env('map.input.file') to get the name of the
file a mapper is reading input from.

If the name of the jobconf variable is different in different
versions of Hadoop (e.g. in Hadoop 2.0, map.input.file is
mapreduce.map.input.file), we'll automatically try all
variants before giving up.

Return *default* if that jobconf variable isn't set.
"""

class KMeansJob(MRJob): # , dim=None):
    # how to pass the parameters in mapper step
    # We can do run loop when calling hadoop
    # it might not be necessary

    """
    def __init__(self, dim):
        self.dimensions = dim
    """

    def configure_args(self):
        super(KMeansJob, self).configure_args()
        self.add_file_arg("--centroids")
    
    def retrieveCentroids(self, file):
        with open(file, "r") as inputFile:
            output_data = inputFile.readlines()
        centroids = []
        for line in output_data:
            line = line.split(';')
            untreated_str_coords = line[1]
            str_coords = ''.join(c for c in str(untreated_str_coords) if c not in '[ ]')
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
    
    def mapper_init(self):
        self.dimensions = self.retrieveCentroids(self.options.centroids)

    def mapper(self, _, line): # mapper, key, record
        # self.dimensions = self.retrieveCentroids(self.options.centroids)
        line = line.strip() # remove leading and trailing whitespace
        l_array = line.split(',')
        try:
            x_coord = float(l_array[-2].strip())
        except ValueError:
            x_coord = float(0)
        
        try:
            y_coord = float(l_array[-1].strip())
        except ValueError:
            y_coord = float(0)

        new_coord = [x_coord, y_coord]

        min_dist = math.inf
        closest_centroid = -1

        for c in self.dimensions:
            distance = math.sqrt(pow(new_coord[0]-c[0], 2) + pow(new_coord[1] - c[1], 2)) 
            if distance < min_dist:
                closest_centroid = self.dimensions.index(c)
                min_dist = distance
        yield closest_centroid, new_coord
    
    def reducer_init(self):
        self.r_dimensions = self.retrieveCentroids(self.options.centroids)

    def reducer(self, key, values):
        # dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
        centroids = self.r_dimensions
        final_value = centroids[(key-1)]
        num_points = 0

        for v in values:
            num_points += 1
            new_average_x = (num_points * final_value[0] + v[0]) / (num_points + 1)
            new_average_y = (num_points * final_value[1] + v[1]) / (num_points + 1)
            final_value = [new_average_x, new_average_y]
        final_value = ";  " + str(final_value) + " ; " + str(num_points)
        yield key, final_value

if __name__ == "__main__":
    """
    dim = [[41.98131263, -87.806945473],
    [41.771488695, -87.667641182],
    [41.884494554, -87.627138636],
    [41.754594962, -87.70872738],
    [41.840581183804865, -87.67204270608761]]
    """
    # dim = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
    KMeansJob.run() # dim)