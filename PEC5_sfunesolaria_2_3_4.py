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
    .master("local[4]") \
    .getOrCreate()

linesDF = spark\
    .readStream\
    .format('socket')\
    .option('host', 'localhost')\
    .option('port', 20068)\
    .load()

wordsDF = linesDF.withColumn("value", linesDF.value)

from pyspark.sql.types import StringType, DoubleType, StructType, StructField
from pyspark.sql.functions import from_json, col, asc
jsonSchema = StructType([ StructField("callsign", StringType(), True),
                          StructField("velocity", DoubleType(), True),
                          StructField("longitude", DoubleType(), True),
                          StructField("latitude", DoubleType(), True),
                          StructField("country", StringType(), True),
                          StructField("vertical_rate", DoubleType(), True)
                        ])

df = wordsDF.withColumn("value", from_json(col("value"), jsonSchema))
df_select = df.select("value.country").groupBy('country').count()

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
    display(spark.sql('SELECT country, count FROM palabras').orderBy(asc("country")).show())
    sleep(5)
