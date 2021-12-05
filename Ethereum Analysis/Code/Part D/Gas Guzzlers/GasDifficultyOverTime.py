from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class GasDifficultyOverTime(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper = self.mapper_repart_join,
                reducer = self.reducer_repart_join
            ),
            MRStep(
                mapper = self.mapper_avg,
                combiner = self.combiner_avg,
                reducer = self.reducer_avg
            )
        ]

    def mapper_repart_join(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 5:
                blockNumber = fields[3]
                yield(blockNumber, (1, 1, 1))
            elif len(fields) == 9:
                blockNumber = fields[0]
                difficulty = int(fields[3])
                epoch_time = int(fields[7])
                time_format = time.strftime('%m/%Y', time.gmtime(epoch_time))
                yield(blockNumber, (str(time_format), difficulty, 2))
        except:
            pass

    def reducer_repart_join(self, blockNumber, values):
        flag = False
        date = ""
        difficulty = 0
        for value in values:
            if value[2] == 1:
                flag = True
            elif value[2] == 2:
                date = value[0]
                difficulty = value[1]

        if flag == True and difficulty != 0:
            yield(date, difficulty)

    def mapper_avg(self, date, difficulty):
        yield(date, (difficulty, 1))

    def combiner_avg(self, date, values):
        count = 0
        total = 0
        for value in values:
            count += value[0]
            total += value[1]
        yield(date, (count, total))

    def reducer_avg(self, date, values):
        count = 0
        total = 0
        for value in values:
            count += value[0]
            total += value[1]
        yield(date, count/total)


if __name__ == "__main__":
    GasDifficultyOverTime.run()