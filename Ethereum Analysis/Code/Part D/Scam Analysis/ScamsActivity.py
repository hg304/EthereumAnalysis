from mrjob.job import MRJob
from mrjob.step import MRStep
import operator

"""
    THIS IS RUN LOCALLY WITH scams.csv, DO NOT RUN WITH CLUSTER
"""

class ScamsActivity(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper = self.mapper_agg,
                combiner = self.combiner_agg,
                reducer = self.reducer_agg
            ),
            MRStep(
                mapper = self.mapper_ranked,
                combiner = self.combiner_ranked,
                reducer = self.reducer_ranked
            )
        ]
    def mapper_agg(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 4:
                category = fields[2]
                activity = fields[3]
                if category == "Scamming":
                    yield((category, activity), 1)
        except:
            pass

    def combiner_agg(self, keys, value):
        yield(keys, sum(value))

    def reducer_agg(self, keys, value):
        yield(keys, sum(value))

    def mapper_ranked(self, keys, value):
        yield(None, (keys, value))
    
    def combiner_ranked(self, _, values):
        sorted_values = sorted(values, reverse=True, key=operator.itemgetter(1))
        for value in sorted_values:
            yield("top", (value[0], value[1]))
    
    def reducer_ranked(self, _, values):
        sorted_values = sorted(values, reverse=True, key=operator.itemgetter(1))
        for value in sorted_values:
            yield(value[0], value[1])

if __name__ == "__main__":
    ScamsActivity.run()