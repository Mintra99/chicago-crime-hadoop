"""
1st attempt: Drop combiner

dbsmasters github:
K-means using Map Reduce [1]
- 1st choose centroids
- Centroids broadcast to all nodes
- yield centroid, x_and_y_coord
K-means using Map Reduce [2]
- Combine to partialsum
- Not relevant for us
K-means Reducer
- yield: centroid, new mean




    Guess
1st yield: id, Point
    1, new_coord
    2, new_coord
1st reducer: cluster, Point
    c1, new_coord
    c1, new_coord

2nd yield: id, Cluster ID
    1, cluster_1
    2, cluster_1




https://www.youtube.com/watch?v=wPcR2aM9upw


fit and predict
fit_predict(df[['Age', 'Income']])
- lag cluster fra age og income


Scale x og y verdier slik at begge er fra 0-1
k-means algorithm to train scale

https://www.dummies.com/article/technology/information-technology/ai/machine-learning/how-to-use-k-means-cluster-algorithms-in-predictive-analysis-154328/



"""

"""
https://hadoopist.wordpress.com/2015/10/01/how-to-broadcast-a-hdfs-file-and-convert-the-values-to-schemardd-in-spark/
Let me explain what is Broadcast Variable in Spark:

Broadcast variables are the ones which allows the program to efficiently send a large, read only value to all Worker Nodes for use in one or more Spark Operations.

For example, Take any Static or Lookup Hive Tables.
"""

"""
https://towardsdatascience.com/chaining-multiple-mapreduce-jobs-with-hadoop-java-832a326cbfa7
Recap: MAp filters while reduce performs a summary operation.

How to chain multiple mapreduce jobs:
Using the Taxi & Limousine Commissio dataset of around 7.6 million rows
to compute the distribution of degree differences of locations

Let us say they want to obtain the output of the following two columns:
- diff(location), count(frequency of a particular location)
Option: We might have 2 jobs, one to clculate the diff, another to
turn the output of the first job into a count

That count is then passed as input for job 2

The challenge is chaining these two MapReduce jobsso that job 2 can
take the output of job 1 without the need for job 1 to physically write out
a file

Basically, the key is to create two different configurations for the two jobs
as conf and conf 2 where they also get two different instances.
Then, job 2 can only be completed after job 1 because "job1.waitforcompletion"

We must manually set the input path of job 2 to be the output of job 1

"""

"""
Chaining jobs:
https://blog.matthewrathbone.com/2015/06/25/real-world-hadoop-implementing-a-left-outer-join-in-java-with-cascading.html

Joining two datasets together

We have:
    User information (id, email, language, location)
    Transaction information (transaction-id, product-id, user-id, purchase-amount, item-description)


In Java:
https://blog.matthewrathbone.com/2013/02/09/real-world-hadoop-implementing-a-left-outer-join-in-hadoop-map-reduce.html

The Map Reduce Solution

First off, the problem requires that we write a two stage map-reduce:
    1. Join users onto transactions and emit a set of product-location
    pairs (one for each transaction)
    2. For each product sold, count the # of distinct locations it was sold in

The Scalable Solution

We do not want our reducer to scan through all values in order to find a
location record. The easiest way to avoid this is to ensure that the first
value in the iterator is the user's location.

Reflecting on the design of map reduce, remember that between map and reduce,
three other things happen:

    1. Partitioning - this defines which reducer partition the record is 
    assigned to
    2. Sorting - the order of the records in the partition, based on key.
    3. Grouping - whether record N+1 accompanies record N in the same call to
    the reduce() function. Again based on key.

By using a compoiste key, we can perform partitioning anf grouping on the primary
key yet yet be able to sort by the secodary key.

First we need a class to represent the composite key. In stage-1 this key would
contain:

    The primary key: userId (for partitioning, and grouping)
    The secondary key: a single character (for sorting)
"""

"""
https://www.youtube.com/watch?v=vswrfVkP10Y

Join is one of the most common reasons the data set is slowed down

Spart: 1 Shuffle Join, 2. Broadcast Join

Let us assumes the 2 data sets have 3 partitions
6 partitions.

We have 3 executors and so eachhas 2 partitions.

Mapping.

In reduction magic happens. Collect all the records for the same key.
These exchanges are known as shuffle partitions.

We assume 3 shuffle partitions because it is the best case scenario.
We have 30 uniqe join keys.
Each reducer exchange gets 10 keys.

This transfer from mapper to reducer is what we call a shuffle operation.
"""

from mrjob.job import MRJob
# from k_clustering_point_class import Cluster
from mrjob.step import MRStep
import math



def getDistance(c, coord):
    x_y_dim = c.getDimensions()
    cur_dist = math.sqrt(pow(coord[0]-x_y_dim[0], 2) + pow(coord[1] - x_y_dim[1], 2)) 
    return cur_dist

"""
A step can run the same program as a preceding step by using
different batch parameters.

You can have virtually as many steps as you want in a
multiple-step job. (There is a maximum number, but it is
more than 32,000—more steps than would be practical for a
job.)

First mapper: filter for gender
Second Mapper: filter for date
"""

class MRCountSum(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_count,
                   reducer=self.reducer_count),
            MRStep(mapper=self.mapper_trans,
                   reducer=self.reducer_trans)
                   ]   
    
    def mapper_init(self):
        self.dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]

    """
    class MAPPER
    method MAP(file_offset, point)
        min_distance = POSITIVE_INFINITY
        closest_centroid = -1
        for all centroid in list_of_centroids
            distance = distance(centroid, point)
            if (distance < min_distance)
                closest_centroid = index_of(centroid)
                min_distance = distance
        EMIT(closest_centroid, point) 

    """

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
        yield closest_centroid, new_coord

    # def combiner(self, key, values):
    #     yield key, sum(values)

    def reducer(self, key, values):
        """
        In the reduce phase, and based on the output of the combiner, you need to recalculate the centroids by
        iterating over the values and output the intermediate centroids. Since we are sharing the centroids among
        iterations, the centroid values have to be updated using the configuration file as stated in the previous
        section.
        """
        yield key, sum(values)

if __name__ == "__main__":
    MRCountSum.run()

"""

dimensions = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]

def getDistance(c, coord):
    x_y_dim = c.getDimensions()
    cur_dist = math.sqrt(pow(coord[0]-x_y_dim[0], 2) + pow(coord[1] - x_y_dim[1], 2)) 
    return cur_dist

class MRCountSum(MRJob):


    def mapper(self, centroids, line):
        line = line.strip() # remove leading and trailing whitespace
        l_array = line.split(',')
        coords = l_array[-1]
        x_coord = float(coords[0])
        y_coord = float(coords[1])
        new_coord = [x_coord, y_coord]

        closest_centroid = -1
        for c in centroids:
            distance = getDistance(c, new_coord)
            if distance < min_dist:
                closest_centroid = centroids.index(c)
                min_dist = distance
        yield new_coord, min_dist

    # def combiner(self, key, values):
    #     yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    MRCountSum.run()
"""