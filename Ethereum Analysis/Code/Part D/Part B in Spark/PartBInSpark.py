import pyspark

sc = pyspark.SparkContext()

def is_good_line_transactions(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        int(fields[3])
        return True

    except:
        return False

def is_good_line_contracts(line):
    try:
        fields = line.split(',')
        if len(fields) != 5 or fields[0] == "address":
            return False
        return True

    except:
        return False

lines_t = sc.textFile("/data/ethereum/transactions")
lines_c = sc.textFile("/data/ethereum/contracts")

cleanlines_t = lines_t.filter(is_good_line_transactions)
cleanlines_c = lines_c.filter(is_good_line_contracts)

features_t = cleanlines_t.map(lambda l: (l.split(',')[2], int(l.split(',')[3])))
features_c = cleanlines_c.map(lambda l: (l.split(',')[0], 0))

tCache = features_t.cache()
cCache = features_c.cache()


grouped = tCache.reduceByKey(lambda a,b: (a+b))
inmem = grouped.persist()

joined = inmem.join(cCache)
final = joined.filter(lambda x: (x[1][1] != None))

top10 = final.takeOrdered(10, key=lambda x: (-x[1][0]))

for record in top10:
    print("{}, {}".format(record[0], record[1][0]))