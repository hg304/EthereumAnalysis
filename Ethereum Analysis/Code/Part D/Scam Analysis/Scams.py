from mrjob.job import MRJob
from mrjob.step import MRStep
import operator

class Scams(MRJob):
    scams_table = {}
    def steps(self):
        return [
            MRStep(
                mapper_init = self.mapper_repl_init,
                mapper = self.mapper_repl_join
            ),
            MRStep(
                mapper = self.mapper_agg,
                combiner = self.combiner_agg,
                reducer = self.reducer_agg
            ),
            MRStep(
                mapper = self.mapper_ranking,
                combiner = self.combiner_ranking,
                reducer = self.reducer_ranking
            )
        ]

    def mapper_repl_init(self):
        with open("scams.csv") as s:
            for line in s:
                fields = line.split(',')
                address = fields[1]
                category = fields[2]
                self.scams_table[address] = category

    def mapper_repl_join(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                address = fields[2]
                if address in self.scams_table:
                    value = int(fields[3])
                    key = self.scams_table[address]
                    if value > 0:
                        yield(key, value)
        except:
            pass

    def mapper_agg(self, category, value):
        yield(category, value)

    def combiner_agg(self, category, value):
        yield(category, sum(value))

    def reducer_agg(self, category, value):
        yield(category, sum(value))

    def mapper_ranking(self, category, value):
        yield(None, (category, value))

    def combiner_ranking(self, _, values):
        sorted_values = sorted(values, reverse=True, key=operator.itemgetter(1))
        rank = 0
        for value in sorted_values:
            yield("top", (value[0], value[1]))
            rank += 1
            if rank >= 10:
                break

    def reducer_ranking(self, _, values):
        sorted_values = sorted(values, reverse=True, key=operator.itemgetter(1))
        rank = 0
        for value in sorted_values:
            yield(value[0], value[1])
            rank += 1
            if rank >= 10:
                break

if __name__ == "__main__":
    Scams.run()