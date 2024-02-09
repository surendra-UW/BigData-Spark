from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":

    # using command-line arguments
    input_path  = sys.argv[1]
    output_path = sys.argv[2]

    # hardcoding variables
    #input_filename_path  = "hdfs://10.10.1.1:9000/web-BerkStan.txt"
    #output_path = "hdfs://10.10.1.1:9000/output2"

    input_workload_name = input_path.split("/")[3]
    # The entry point (SparkSession class)
    spark = (SparkSession
        .builder
        .appName("PageRank_task3_" + input_workload_name)
        #.config("some.config.option", "some-value")
        #.master("spark://c220g5-110927vm-1.wisc.cloudlab.us:7077")
        .getOrCreate())

    sc = spark.sparkContext

    # Get web graph data into RDD
    webNodes = sc.textFile(input_path)

    # filtering out first four lines that containts comments about the dataset
    curatedNodes = webNodes.filter(lambda x: not x.startswith("#"))

    numPartitions = int(sys.argv[3])
    # Generates an RDD of (source, destinations) pairs
    links = curatedNodes.map(lambda row: (row.split("\t")[0], row.split("\t")[1])).partitionBy(numPartitions).groupByKey().persist()

    # Generates an RDD of (source, rank) pairs. Initializes each rank to 1.0
    ranks = links.map(lambda link: (link[0], 1.0))

    # Page Rank algorithm
    MAX_ITER = 5 #number of iterations
    for i in range(MAX_ITER):
        # Compute contribution for every destination (page p contribute rank_p / neighbors_p ) . It generates a mapping (destination, contribution)
        contributions = links.join(ranks).flatMap(
            lambda x: [(destination, x[1][1] / len(x[1][0])) for destination in x[1][0]]
        )
        # Aggregate the contributions and recalculate the rank using given formula
        ranks = contributions.reduceByKey(lambda x, y: x + y).mapValues(lambda sum: 0.15 + (0.85 * sum)).persist()

    # For debug only: Print all pageranks
    #for (link, rank) in ranks.collect():
    #    print("%s has rank: %s." % (link, rank))
    ranks.saveAsTextFile(output_path)

    sc.stop()
