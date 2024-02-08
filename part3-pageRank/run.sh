#!/bin/sh
~/spark-3.3.4-bin-hadoop3/bin/spark-submit --master spark://c220g5-110927vm-1.wisc.cloudlab.us:7077 pagerank.py hdfs://10.10.1.1:9000/web-BerkStan.txt hdfs://10.10.1.1:9000/output