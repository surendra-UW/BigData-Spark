#!/bin/sh

# small workload
#INPUT_PATH="hdfs://10.10.1.1:9000/web-BerkStan.txt"
#OUTPUT_PATH="hdfs://10.10.1.1:9000/output-pagerank-task1-web-BerkStan.txt"

# big workload
INPUT_PATH="hdfs://10.10.1.1:9000/enwiki-pages-articles"
OUTPUT_PATH="hdfs://10.10.1.1:9000/output-pagerank-task1-enwiki-pages-articles"
~/hadoop-3.3.6/bin/hdfs dfs -test -d $OUTPUT_PATH && ~/hadoop-3.3.6/bin/hdfs dfs -rm -r $OUTPUT_PATH
~/spark-3.3.4-bin-hadoop3/bin/spark-submit --master spark://c220g5-110927vm-1.wisc.cloudlab.us:7077 pagerank_task1.py $INPUT_PATH $OUTPUT_PATH