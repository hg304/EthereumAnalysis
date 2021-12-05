from mrjob.job import MRJob
from mrjob.step import MRStep
import operator

class PartC(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_agg,
                combiner=self.combiner_agg,
                reducer=self.reducer_agg
            ),
            MRStep(
                mapper=self.mapper_top_10,
                combiner=self.combiner_top_10,
                reducer=self.reducer_top_10
            )
        ]

    def mapper_agg(self, _, line):
        try:
            if len(line.split(',')) == 9:
                fields = line.split(',')
                address = fields[2]
                size = int(fields[4])
                if size > 0:
                    yield(address, size)
        except:
            pass

    def combiner_agg(self, address, size):
        total = sum(size)
        yield(address, total)

    def reducer_agg(self, address, size):
        total = sum(size)
        yield(address, total)

    def mapper_top_10(self, address, size):
        yield(None, (address, size))

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
    PartC.run()