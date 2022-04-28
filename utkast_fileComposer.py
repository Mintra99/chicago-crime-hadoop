from mrjob.job import MRJob
# from k_clustering_point_class import Cluster
# from mrjob.step import MRStep
import math
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.compat import jobconf_from_env
import sys

class KMeansJob(MRJob): # , dim=None):

    # WE enable the file passthroughargument
    def configure_args(self):
        super(KMeansJob, self).configure_args()
        self.add_file_arg("--centroids")
        # self.add_file_option("--inputFile")
    
    def retrieveCentroids(self, file):
        with open(file, "r") as inputFile:
            output_data = inputFile.readlines()
        centroids = []
        for line in output_data:
            line = line.split(';')
            untreated_str_coords = line[1]
            # print(str(untreated_str_coords.encode()), file=sys.stderr)
            """
            sys.stderr.write('Centroid'.encode('utf-8'))
            sys.stderr.write(untreated_str_coords.encode('utf-8'))
            """
            str_coords = ''.join(c for c in str(untreated_str_coords) if c not in '[ ]')
            # print(str_coords, file=sys.stderr)
            coords = str_coords.split(',')
            # print(coords, file=sys.stderr)
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
    
    # I mapper innit lag en kobling til databasen
    def mapper_init(self):
        self.dimensions = self.retrieveCentroids(self.options.centroids)
    # I mapper insert til databasen

    def mapper(self, _, line): # mapper, key, record
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

        # dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
        for c in self.dimensions:
            distance = math.sqrt(pow(new_coord[0]-c[0], 2) + pow(new_coord[1] - c[1], 2)) 
            if distance < min_dist:
                closest_centroid = self.dimensions.index(c)
                min_dist = distance
        string = " ; " + str(new_coord) + " ; 1 "
        yield closest_centroid, string

    def get_int_coord(self, v):
        v_info = ''.join(c for c in str(v) if c not in '[ "]')
        coord_num = v_info.split(';')
        # num = int(coord_num[2])
        # num_points += num

        coords = coord_num[1].split(',')
        try:
            x_coord = float(coords[0].strip())
        except ValueError:
            x_coord = float(0)

        try:
            y_coord = float(coords[1].strip())
        except ValueError:
            y_coord = float(0)
        new_coord = [x_coord, y_coord]
        return new_coord
    
    """
    def composer_init(self):
        self.comp_dimensions = self.retrieveCentroids(self.options.centroids)
    """

    def composer(self, key, values):
        self.comp_dimensions = self.retrieveCentroids(self.options.centroids)
        # final_value = self.comp_dimensions[(key-1)]
        # dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
        num_points = 0
        final_x = 0
        final_y = 0
        
        for v in values:
            coord = self.get_int_coord(v)
            num_points += 1
            final_x += coord[0]
            final_y += coord[1]
        final_value = [final_x, final_y]
        final_value = ";  " + str(final_value) + " ; " + str(num_points)
        yield key, final_value

    def getWeightAndCoord(self, v):
        v_info = ''.join(c for c in str(v) if c not in '[ )"]')
        coord_num = v_info.split(';')
        list_weight = int(coord_num[2])

        coords = coord_num[1].split(',')
        try:
            x_coord = float(coords[0].strip())
        except ValueError:
            x_coord = float(0)

        try:
            y_coord = float(coords[1].strip())
        except ValueError:
            y_coord = float(0)
        
        new_coord = [x_coord, y_coord]
        return list_weight, new_coord
    
    # I mapper innit lag en kobling til databasen
    def reducer_init(self):
        self.r_centroids = self.retrieveCentroids(self.options.centroids)
    # I mapper insert til databasen

    def reducer(self, key, values):
        final_value = self.r_centroids[(key-1)]
        num_points = 1
        for v in values: #[2:]:
            list_weight, new_coord = self.getWeightAndCoord(v)
            num_points += list_weight
            final_value[0] += new_coord[0]
            final_value[1] += new_coord[1]

        new_average_x = (final_value[0]) / (num_points)
        new_average_y = (final_value[1]) / (num_points)
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
