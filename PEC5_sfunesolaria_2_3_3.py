import findspark
findspark.init()

from pyspark import SparkConf, SparkContext, SQLContext, HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json

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

#query = wordCountsDF\
#    .writeStream\
#    .outputMode('complete')\
#    .format("memory") \
#    .queryName("palabras") \
#    .start()

#query.awaitTermination() 

from pyspark.sql.types import StringType, LongType, StructType, StructField
from pyspark.sql.functions import from_json, col
jsonSchema = StructType([ StructField("callsign", StringType(), True),
                          StructField("velocity", StringType(), True),
                          StructField("longitude", StringType(), True),
                          StructField("latitude", StringType(), True),
                          StructField("country", StringType(), True),
                          StructField("vertical_rate", StringType(), True)
                        ])

df = wordCountsDF.withColumn("value", from_json(col("value"), jsonSchema))
df_select = df.select("value.callsign", "value.country", "value.longitude", "value.latitude", "value.velocity",
                      "value.vertical_rate")

query = df_select\
    .writeStream\
    .outputMode('complete')\
    .format("memory") \
    .queryName("palabras") \
    .start()

from IPython.display import display, clear_output
from time import sleep
while True:
    clear_output(wait=True)
#    display(query.status)
    display(df_select.printSchema())
    display(spark.sql('SELECT country, callsign, longitude, latitude, velocity, vertical_rate FROM palabras').show())
    sleep(5)