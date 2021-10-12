import sys
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import *
from pyspark.sql import SparkSession

source=sys.argv[1]
target=sys.argv[2]

spark = SparkSession\
    .builder\
    .appName("repartition-job")\
    .getOrCreate()

jsonOptions = { "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss" }

# Define schema of json
spark = SparkSession\
        .builder\
        .appName("Repartition Job")\
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = (
    spark
    .read
    .format("csv")
    .options(header='true', inferSchema='true', delimiter=';')
    .load(source)
)


df.show()
df.printSchema()

(df
.write
.mode("overwrite")
.format("parquet")
.save(target)
)

print("*****************")
print("Escrito com sucesso!")
print("*****************")

spark.stop()