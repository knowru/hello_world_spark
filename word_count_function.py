from operator import add

def word_count(data_rdd):
    counts = data_rdd.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).collect()
    return counts