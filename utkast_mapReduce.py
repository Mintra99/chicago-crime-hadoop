from mrjob.job import MRJob
# from k_clustering_point_class import Cluster
from mrjob.step import MRStep
import math



def getDistance(c, coord):
    x_y_dim = c.getDimensions()
    cur_dist = math.sqrt(pow(coord[0]-x_y_dim[0], 2) + pow(coord[1] - x_y_dim[1], 2)) 
    return cur_dist

class MRCountSum(MRJob):
    # how to pass the parameters in mapper step
    # We can do run loop when calling hadoop
    # it might not be necessary
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
                   ]
    
    
    # for loop when we call mapreduce
    def mapper_init(self):
        self.dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]

    def mapper(self, _, line): # mapper, key, record
        line = line.strip() # remove leading and trailing whitespace
        l_array = line.split(',')
        coords = l_array[-1]
        x_coord = float(coords[0])
        y_coord = float(coords[1])
        new_coord = [x_coord, y_coord]

        min_dist = math.inf
        closest_centroid = -1
        for c in self.dimensions:
            distance = getDistance(c, new_coord)
            if distance < min_dist:
                closest_centroid = self.dimensions.index(c)
                min_dist = distance
        val = [1, new_coord]
        yield closest_centroid, val

    # def combiner(self, key, values):
    #     yield key, sum(values)

    def reducer(self, key, values):
        """
        In the reduce phase, and based on the output of the combiner, you need to recalculate the centroids by
        iterating over the values and output the intermediate centroids. Since we are sharing the centroids among
        iterations, the centroid values have to be updated using the configuration file as stated in the previous
        section.
        """
        num_points = 0
        final_value = [0, 0]

        for v in values:
            # opdater punktteller
            point_num = values[0]
            num_points += point_num

            point_coord = values[1]
            # new_average_x = (self.num_points * self.x_y_dimensions[0] + coord[0]) / (self.num_points + 1)
            new_average_x = (num_points * final_value[0] + point_coord[0]) / (num_points + 1)
            new_average_y = (num_points * final_value[1] + point_coord[1]) / (num_points + 1)
            final_value = [new_average_x, new_average_y]
        yield key, final_value

if __name__ == "__main__":
    MRCountSum.run()