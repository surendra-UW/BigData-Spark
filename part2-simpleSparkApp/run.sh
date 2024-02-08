#!/bin/sh
OUTPUT_PATH="hdfs://10.10.1.1:9000/output-simpleSparkApp"
~/spark-3.3.4-bin-hadoop3/bin/spark-submit --master spark://c220g5-110927vm-1.wisc.cloudlab.us:7077 simpleApp.py hdfs://10.10.1.1:9000/export.csv $OUTPUT_PATH