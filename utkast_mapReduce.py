from mrjob.job import MRJob
# from k_clustering_point_class import Cluster
# from mrjob.step import MRStep
import math
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.compat import jobconf_from_env

class KMeansJob(MRJob): # , dim=None):
    # how to pass the parameters in mapper step
    # We can do run loop when calling hadoop
    # it might not be necessary
    
    def configure_args(self):
        super(KMeansJob, self).configure_args()
        self.add_passthru_arg(
            '--output-format', default='raw', choices=['raw', 'json'],
            help="Specify the output format of the job")

    def output_protocol(self):
        if self.options.output_format == 'json':
            return JSONValueProtocol()
        elif self.options.output_format == 'raw':
            return RawValueProtocol()

    def mapper_init(self, dim=[[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]):
        jobconf = jobconf_from_env("my.job.settings.starting_values")
        string_line = ''.join(c for c in jobconf if c not in '[ ]')
        string_array = string_line.split(',')
        dimensions = []
        centroid = []
        for i in range(len(string_array)):
            number = float(string_array[i].strip())
            if (i%2) == 0:
                centroid.append(number)
            elif (i%2) == 1:
                centroid.append(number)
                dimensions.append(centroid)
                centroid = []
        self.dimensions = dimensions

    """
    def __init__(self, dim=[[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]):
        self.dimensions = dim
    """

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
        yield closest_centroid, new_coord

    def reducer_init(self, dim=[[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]):
        jobconf = jobconf_from_env("my.job.settings.starting_values")
        string_line = ''.join(c for c in jobconf if c not in '[ ]')
        string_array = string_line.split(',')
        dimensions = []
        centroid = []
        for i in range(len(string_array)):
            number = float(string_array[i].strip())
            if (i%2) == 0:
                centroid.append(number)
            elif (i%2) == 1:
                centroid.append(number)
                dimensions.append(centroid)
                centroid = []
        self.dimensions = dimensions

    def reducer(self, key, values):
        # dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
        final_value = self.dimensions[(key-1)]
        num_points = 0
        for v in values: #[2:]:
            # opdater punktteller
            num_points += 1
            # new_average_x = (self.num_points * self.x_y_dimensions[0] + coord[0]) / (self.num_points + 1)
            new_average_x = (num_points * final_value[0] + v[0]) / (num_points + 1)
            new_average_y = (num_points * final_value[1] + v[1]) / (num_points + 1)
            final_value = [new_average_x, new_average_y]
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