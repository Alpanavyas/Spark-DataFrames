from pyspark.sql import SparkSession

from pyspark.sql.types import StringType,StructField,StructType,IntegerType
from pyspark.sql.functions import col,row_number,rank,dense_rank,ntile,lag,avg,explode

if __name__ == '__main__':
    # conf = SparkConf()
    # sc = SparkContext(conf=conf)
    spark = SparkSession.builder\
                .appName('Read and Write avro')\
                .master('local[3]')\
                .config('spark.jars','/home/saif/LFS/jars/spark-avro_2.12-3.0.1.jar')\
                .getOrCreate()

    df = spark.read.format('avro').load('/home/saif/LFS/datasets/users.avro')

    df.show(truncate=False)
    df.printSchema()