import sys
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: ['JOB_NAME']
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ler os dados do enade

enade = (
    spark
    .read
    .option("header", True)
    .option("delimiter", ";")
    .option("inferSchema", True)
    .csv("s3://datalake-brx-edc/raw-data/enade/enade2017.txt")
)

# transformar em parquet

(
    enade
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-brx-edc/staging/enade/")    
)
