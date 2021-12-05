from mrjob.job import MRJob
from mrjob.step import MRStep
import operator

class ForkAfterMostLucrative(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper = self.mapper_agg,
                combiner = self.combiner_agg,
                reducer = self.reducer_agg
            ),
            MRStep(
                mapper = self.mapper_top_10,
                combiner = self.combiner_top_10,
                reducer = self.reducer_top_10
            )
        ]

    def mapper_agg(self, _, line):
        fields = line.split(',')
        try:
            if len(fields) == 7:
                address = fields[2]
                value = int(fields[3])
                epoch_time = int(fields[6])
                if epoch_time >= 1508131331:
                    yield(address, value)
        except:
            pass

    def combiner_agg(self, address, value):
        yield(address, sum(value))

    def reducer_agg(self, address, value):
        yield(address, sum(value))

    def mapper_top_10(self, address, value):
        yield(None, (address, value))

    def combiner_top_10(self, _, values):
        sorted_values = sorted(values, reverse=True, key=operator.itemgetter(1))
        rank = 0
        for value in sorted_values:
            yield("top", (value[0], value[1]))
            rank += 1
            if rank >= 10:
                break

    def reducer_top_10(self, _, values):
        sorted_values = sorted(values, reverse=True, key=operator.itemgetter(1))
        rank = 0
        for value in sorted_values:
            yield(value[0], value[1])
            rank += 1
            if rank >= 10:
                break

if __name__ == "__main__":
    ForkAfterMostLucrative.run()
