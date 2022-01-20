from pyspark import SparkConf, SparkContext


def parse_edge(edge):
    user, follower = edge.split('\t')
    return (int(user), int(follower))

def step(item):
    # Function to recalc data after join. 
    # Takes (K, (W, V)) where W is already constructed path, V is a new vertice 
    prev_last, path, next_last = item[0], item[1][0], item[1][1]
    return (next_last, path + [next_last])


config = SparkConf().setAppName('sparktask2_pd2021075').setMaster('yarn')
sc = SparkContext(conf=config)


n = 100  # number of partitions
start_v, terminal_v = 12, 34


edges = sc.textFile('/data/twitter/twitter_sample.txt').map(parse_edge).cache()
forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(n).cache()

# we will accumulate result in such format
# (last_vertex_in_path, [path]) - last_vertex is used to join on it
paths = sc.parallelize([(start_v, [start_v])]).partitionBy(n)

while not paths.filter(lambda x: x[0] == terminal_v).count():
    paths = paths.join(forward_edges, n).map(step)

# gets [paths] arrays which have last_vertex_in_path == terminal_v
# we need only one 
result = paths.filter(lambda x: x[0] == terminal_v).map(lambda x: x[1]).take(1)

print(','.join(map(str, result[0])))
