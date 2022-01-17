import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

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

from pyspark.sql.types import StringType, DoubleType, StructType, StructField
from pyspark.sql.functions import from_json, col
jsonSchema = StructType([ StructField("callsign", StringType(), True),
                          StructField("velocity", DoubleType(), True),
                          StructField("longitude", DoubleType(), True),
                          StructField("latitude", DoubleType(), True),
                          StructField("country", StringType(), True),
                          StructField("vertical_rate", DoubleType(), True)
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
    display(df_select.printSchema())
    display(spark.sql('SELECT country, callsign, longitude, latitude, velocity, vertical_rate FROM palabras').show())
    sleep(5)
