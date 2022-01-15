import findspark
findspark.init()

from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

conf = SparkConf()
#conf = SparkConf().set("spark.ui.showConsoleProgress", "false")
conf.setMaster("local[1]")
sc = SparkContext(conf=conf)
print(sc.version)

# Introducid el nombre de la app PEC5_ seguido de vuestro nombre de usuario
spark = SparkSession \
    .builder \
    .appName("PEC5_sfunesolaria") \
    .getOrCreate()

# Creamos el DataFrame representando el streaming de las lineas que nos entran por host:port
linesDF = spark\
    .readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 20068)\
    .load()

# Separamos las lineas en palabras en un nuevo DF
#las funciones explode y split estan explicadas en
#https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html
wordsDF = linesDF.withColumn("value", linesDF.value)

wordCountsDF = wordsDF.groupBy('value').count()

query = wordCountsDF\
    .writeStream\
    .outputMode('complete')\
    .format("memory") \
    .queryName("palabras") \
    .start()

#query.awaitTermination() 

from IPython.display import display, clear_output
from time import sleep
while True:
    clear_output(wait=True)
    display(wordsDF.printSchema())
#    display(query.status)
    display(spark.sql('SELECT value FROM palabras').show())
    sleep(5)