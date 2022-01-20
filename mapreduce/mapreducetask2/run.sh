#!/usr/bin/env bash

OUT_DIR="pd2021075_task_114"
OUT_DIR_TMP="pd2021075_task_114_tmp"
NUM_REDUCERS=4

hadoop fs -rm -r -skipTrash $OUT_DIR_TMP > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /data/wiki/en_articles \
    -output $OUT_DIR_TMP > /dev/null

hadoop fs -rm -r -skipTrash $OUT_DIR > /dev/null

hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D stream.num.map.output.key.fields=3 \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options='-k2,2nr -k1' \
    -mapper cat \
    -reducer cat \
    -input $OUT_DIR_TMP \
    -output $OUT_DIR > /dev/null

hdfs dfs -cat $OUT_DIR/part-0000* | head -10
