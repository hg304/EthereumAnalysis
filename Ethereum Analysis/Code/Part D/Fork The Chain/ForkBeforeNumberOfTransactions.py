from mrjob.job import MRJob
import time

class ForkBeforeNumberOfTransactions(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')
        try:
            if len(fields) == 7:
                epoch_time = int(fields[6])
                time_format = time.strftime('%m/%Y', time.gmtime(epoch_time))
                if epoch_time < 1508131331:
                  yield(str(time_format), 1)
        except:
            pass

    def combiner(self, date, value):
        yield(date, sum(value))

    def reducer(self, date, value):
        yield(date, sum(value))

if __name__ == "__main__":
    ForkBeforeNumberOfTransactions.run()