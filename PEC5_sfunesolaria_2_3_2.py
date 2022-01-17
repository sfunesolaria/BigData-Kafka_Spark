import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf()
conf.setMaster("local[1]")
sc = SparkContext(conf=conf)
print(sc.version)

spark = SparkSession \
    .builder \
    .appName("PEC5_sfunesolaria") \
    .getOrCreate()

linesDF = spark\
    .readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 20068)\
    .load()

wordsDF = linesDF.withColumn("value", linesDF.value)

wordCountsDF = wordsDF.groupBy('value').count()

query = wordCountsDF\
    .writeStream\
    .outputMode('complete')\
    .format("memory") \
    .queryName("palabras") \
    .start()

from IPython.display import display, clear_output
from time import sleep
while True:
    clear_output(wait=True)
    display(wordsDF.printSchema())
    display(spark.sql('SELECT value FROM palabras').show())
    sleep(5)
