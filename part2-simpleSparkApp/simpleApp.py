from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":

	# using command-line arguments
	input_filename_path  = sys.argv[1]
	output_path = sys.argv[2]

	# hardcoding variables
	#input_filename_path  = "hdfs://10.10.1.1:9000/export.csv"
	#output_path = "hdfs://10.10.1.1:9000/output"


	# The entry point (SparkSession class)
	spark = (SparkSession
		.builder
		.appName("FirstApp")
		#.config("some.config.option", "some-value")
		#.master("spark://c220g5-110927vm-1.wisc.cloudlab.us:7077")
		.getOrCreate())

	# Read file into DataFrames
	df = spark.read.csv(input_filename_path,
						header='true')

	# Sorting (stable sorting) the data firth by country code ("cca2" column) and then by timestamp ("timestamp" column)
	df.orderBy(['cca2', 'timestamp'], 
				ascending=True).show()
	#df.show()
	df.printSchema()
	df.coalesce(1).write.csv(output_path, mode="overwrite", header=True, sep=',')