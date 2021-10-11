from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
)

sc = SparkContext(conf=conf).getOrCreate()


if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

### Lendo curso.csv
    df = (
        spark
        .read
        .format("csv")
        .options(header=True, inferSchema=True, delimiter="|", encoding="latin1")
        .load("s3a://datalake-brx-edc/landing-zone/edsup2019/curso/")
    )

### Escrevendo em formato parquet 
    (
        df
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://datalake-brx-edc/processing/edsup2019/curso/")    
    )
