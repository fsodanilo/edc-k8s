from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# set conf
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()
    

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("ENADE Job")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df_enade = (
        spark
        .read
        .option("header", True)
        .option("inferSchema", True)
        .option("delimiter", ";")
        .load("s3a://datalake-brx-edc/staging/enade")
    )

    nota_trans = (
        df_enade
        .withColumn('NF_GER_FMT', f.regexp_replace(f.col('NT_GER'), ',', '.').cast('double'))
    )
    
    (
        nota_trans
        .write
        .mode("overwrite")
        .format("parquet")
        .save("s3a://datalake-brx-edc/consumer/enade")    
    )


    print("*********************")
    print("convertido com sucesso")
    print("*********************")

    spark.stop()