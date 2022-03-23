from mrjob.job import MRJob

class MRCountSum(MRJob):

    def mapper(self, _, line):
        line = line.strip() # remove leading and trailing whitespace
        
        l_array = line.split(',')
        type_of_crime = l_array[3]
        """
        month = date[0:2]
        day = date[3:5]
        hour = int(date[11:13])
        AM_or_PM = date[20:22]
        if AM_or_PM == "PM":
            new_hour = hour + 12
        elif AM_or_PM == "AM":
            new_hour = hour
        day_month_hour = day + "/"+ month + ":" + str(new_hour)
        """
        yield type_of_crime, 1


    def combiner(self, key, values):
        yield key, sum(values)
        
    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    MRCountSum.run()