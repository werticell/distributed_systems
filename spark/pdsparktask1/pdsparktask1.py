from pyspark.sql import SparkSession
import re

spark_session = SparkSession.builder.master("yarn") \
                .appName("sparktask1_pd2021075") \
                .config("spark.ui.port", "18089") \
                .getOrCreate()


rdd = spark_session.sparkContext.textFile('/data/wiki/en_articles_part')
# lower case letters, remove redundant spaces and id
rdd = rdd.map(lambda x: x.strip().lower().split('\t', 1)[1])
# remove non letter symbols and split
rdd = rdd.map(lambda x: re.sub("\W+", " ", x).split(' '))
# create table of all text pairs
rdd = rdd.flatMap(lambda x: list(zip(x[:-1], x[1:])))
# filter by condition
rdd = rdd.filter(lambda x: x[0] == 'narodnaya')
# wordcount
rdd = rdd.map(lambda x: (x, 1))
rdd = rdd.reduceByKey(lambda a, b: a + b)
rdd = rdd.sortByKey()

result = rdd.collect()

for bigram, count in result:
    print('{}_{} {}'.format(bigram[0], bigram[1], count))
