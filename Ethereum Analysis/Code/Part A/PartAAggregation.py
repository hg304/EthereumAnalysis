from mrjob.job import MRJob
import time

class PartAAggregation(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')   
        try:
            if len(fields) == 7:
                epoch_time = int(fields[6])
                time_format = time.strftime('%m/%Y', time.gmtime(epoch_time))
                yield(time_format, 1)
        except:
            pass

    def combiner(self, date, values):
        total = sum(values)
        yield(date, total)

    def reducer(self, date, values):
        total = sum(values)
        yield(date, total)

if __name__ == "__main__":
    PartAAggregation.run()