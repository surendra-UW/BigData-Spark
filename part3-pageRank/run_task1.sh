#!/bin/sh
OUTPUT_PATH="hdfs://10.10.1.1:9000/output-pagerank-task1"
INPUT_FILENAME="hdfs://10.10.1.1:9000/web-BerkStan.txt"
~/hadoop-3.3.6/bin/hdfs dfs -test -d $OUTPUT_PATH && ~/hadoop-3.3.6/bin/hdfs dfs -rm -r $OUTPUT_PATH
~/spark-3.3.4-bin-hadoop3/bin/spark-submit --master spark://c220g5-110927vm-1.wisc.cloudlab.us:7077 pagerank.py $INPUT_FILENAME $OUTPUT_PATH