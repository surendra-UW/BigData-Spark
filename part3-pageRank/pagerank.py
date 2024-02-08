from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":

    # using command-line arguments
    input_filename_path  = sys.argv[1]
    output_path = sys.argv[2]

    # hardcoding variables
    #input_filename_path  = "hdfs://10.10.1.1:9000/web-BerkStan.txt"
    #output_path = "hdfs://10.10.1.1:9000/output2"

    # The entry point (SparkSession class)
    spark = (SparkSession
        .builder
        .appName("PageRank")
        #.config("some.config.option", "some-value")
        #.master("spark://c220g5-110927vm-1.wisc.cloudlab.us:7077")
        .getOrCreate())

    sc = spark.sparkContext

    # Fetch input and output file locations from input args
    inputFile = input_filename_path
    outputLocation = output_path

    # Get web graph data into RDD
    webNodes = sc.textFile(inputFile)

    # filtering out first four lines that containts comments about the dataset
    curatedNodes = webNodes.filter(lambda x: not x.startswith("#"))

    # Generates RDD with mapping a source: List(destination)
    links = curatedNodes.map(lambda row: (row.split("\t")[0], row.split("\t")[1])).groupByKey()

    # Initializes each rank to 1.0
    ranks = links.map(lambda link: (link[0], 1.0))

    # Page Rank algorithm
    MAX_ITER = 10 #number of iterations
    for i in range(MAX_ITER):
        # Compute contribution for every destination. It generates a mapping destination: contribution
        contributions = links.join(ranks).flatMap(
            lambda x: [(destination, x[1][1] / len(x[1][0])) for destination in x[1][0]]
        )
        # Aggregate the contributions and update the rank using given formula
        ranks = contributions.reduceByKey(lambda x, y: x + y).mapValues(lambda sum: 0.15 + (0.85 * sum))

    # For debug only: Print all pageranks
    #for (link, rank) in ranks.collect():
    #    print("%s has rank: %s." % (link, rank))

    # delete output directory (if exists) before saving new results
    #fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    #if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(outputLocation)):
    #    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(outputLocation))

    # Save RDD as a text file at user provided outputLocation
    ranks.saveAsTextFile(outputLocation)

    sc.stop()