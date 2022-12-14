from mrjob.job import MRJob
from mrjob.step import MRStep
#mapreduce

class MaxPropertyPrice(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_passthrough,
                   reducer=self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        ID,Year,Month,Type,Price = line.split(",")
        yield Year, 1

    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values),key)

    def reducer_find_max(self, key, values):
        yield max(values)
        
if __name__ == '__main__':
    MaxPropertyPrice.run()



