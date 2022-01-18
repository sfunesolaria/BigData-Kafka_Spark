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
    .format('rate')\
    .option('rowsPerSecond', '1')\
    .load()

wordsDF = linesDF.withColumn("window", linesDF.timestamp)\
                .withColumn("value", linesDF.value)

from pyspark.sql.functions import window

wordCountsDF = wordsDF.groupBy(window('window', "10 seconds", "5 seconds"), 'value').count()

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
    display(query.status)
    display(query.lastProgress)
    display(query.recentProgress)
    sleep(5)
