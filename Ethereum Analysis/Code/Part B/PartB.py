from mrjob.job import MRJob
from mrjob.step import MRStep
import operator

class PartB(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_agg_and_join,
                reducer=self.reducer_agg_and_join
            ),
            MRStep(
                mapper=self.mapper_top_10,
                combiner=self.combiner_top_10,
                reducer=self.reducer_top_10
            )
        ]

    def mapper_agg_and_join(self, _, line):
        try:
            if len(line.split(',')) == 5:
                fields = line.split(',')
                address = fields[0]
                yield(address, (1, 1))

            elif len(line.split(',')) == 7:
                fields = line.split(',')
                address = fields[2]
                value = int(fields[3])
                yield(address, (value, 2))
        except:
            pass

    def reducer_agg_and_join(self, address, values):
        vals = []
        flag = False

        for value in values:
            if value[1] == 1:
                flag = True
            elif value[1] == 2:
                vals.append(value[0])

        if flag == True and sum(vals) != 0:
            yield(address, sum(vals))

    def mapper_top_10(self, address, total):
        yield(None, (address, total))

    def combiner_top_10(self, _, values):
        sorted_vals = sorted(values, reverse=True, key=operator.itemgetter(1))
        rank = 0
        for value in sorted_vals:
            yield("top", (value[0], value[1]))
            rank += 1
            if rank >= 10:
                break

    def reducer_top_10(self, _, values):
        sorted_vals = sorted(values, reverse=True, key=operator.itemgetter(1))
        rank = 0
        for value in sorted_vals:
            yield(value[0], value[1])
            rank += 1
            if rank >= 10:
                break


if __name__ == "__main__":
    PartB.run()