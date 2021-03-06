import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

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

from pyspark.sql.functions import current_timestamp

wordsDF = linesDF.select(
    explode(
        split(linesDF.value, ' ')
    ).alias('palabra'), current_timestamp().alias('tiempo')
)

wordCountsDF = wordsDF.groupBy('palabra', 'tiempo').count()

query = wordCountsDF\
    .writeStream\
    .outputMode('complete')\
    .option("checkpointLocation", "/user/sfunesolaria/PEC5/punto_control_pec5") \
    .format("memory") \
    .queryName("palabras") \
    .start()

from IPython.display import display, clear_output
from time import sleep
while True:
    clear_output(wait=True)
    display(query.status)
    display(spark.sql('SELECT palabra, tiempo FROM palabras WHERE LENGTH(palabra) > 3').show())
    sleep(5)
