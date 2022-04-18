from mrjob.job import MRJob
# from k_clustering_point_class import Cluster
from mrjob.step import MRStep
import math

class MRCountSum(MRJob):
    # how to pass the parameters in mapper step
    # We can do run loop when calling hadoop
    # it might not be necessary
    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
                   ]
    """
        

    def mapper(self, _, line): # mapper, key, record
        line = line.strip() # remove leading and trailing whitespace
        l_array = line.split(",")
        l_array[-1] = "".join(c for c in line[-1] if c not in "()")
        coords = line[-1].split(",")
        
        x_coord = float(coords[0])
        y_coord = float(coords[1])
        new_coord = [x_coord, y_coord]

        dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
        min_dist = math.inf
        closest_centroid = -1
        for c in dimensions:
            distance = math.sqrt(pow(new_coord[0]-c[0], 2) + pow(new_coord[1] - c[1], 2)) 
            if distance < min_dist:
                closest_centroid = dimensions.index(c)
                min_dist = distance
        yield closest_centroid, new_coord


    def reducer(self, key, values):
        dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
        final_value = dimensions[(key-1)]
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
    MRCountSum.run()