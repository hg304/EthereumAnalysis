from mrjob.job import MRJob
from mrjob.step import MRStep
import time


class ScamsNumber(MRJob):
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
            )
        ]

    def mapper_repl_init(self):
        with open("scams.csv") as s:
            for line in s:
                fields = line.split(",")
                address = fields[1]
                category = fields[2]
                self.scams_table[address] = category

    def mapper_repl_join(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                address = fields[2]
                value = int(fields[3])
                epoch_time = int(fields[6])
                time_format = time.strftime('%m/%Y', time.gmtime(epoch_time))
                if address in self.scams_table:
                    if self.scams_table[address] == "Scamming":
                        key = time_format
                        if value > 0:
                            yield(key, 1)
        except:
            pass

    def mapper_agg(self, date, value):
        yield(date, value)

    def combiner_agg(self, date, value):
        yield(date, sum(value))

    def reducer_agg(self, date, value):
        yield(date, sum(value))

if __name__ == "__main__":
    ScamsNumber.run()