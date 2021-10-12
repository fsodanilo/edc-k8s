from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# set conf
print("*********************")
print("INCIO DA ROTINA OK")
print("*********************")

conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
)

print("*********************")
print("SPARK CONFIG OK")
print("*********************")
# apply config
sc = SparkContext(conf=conf).getOrCreate()
    

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("ENADE Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark
        .read
        .option("header", True)
        .option("delimiter", ";")
        .option("inferSchema", True)
        .csv("s3a://datalake-brx-edc/raw-data/enade/enade2017.txt")
    )
    
    df.printSchema()

    (
        df
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://datalake-brx-edc/staging/enade")    
    )


    print("*********************")
    print("Escrito com sucesso!")
    print("*********************")

    spark.stop()