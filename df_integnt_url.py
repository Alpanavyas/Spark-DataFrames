from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from urllib.request import urlopen
from pyspark.sql.functions import col,split,explode

if __name__ == '__main__':
    sparkConf = SparkConf
    sc = SparkContext(conf=sparkConf)
    spark = SparkSession.builder \
            .appName('read web url data') \
            .master('local[*]')\
            .getOrCreate()

    readFile = urlopen("https://randomuser.me/api/0.8/?results=5")
    myDF = spark.read.json(sc.parallelize([readFile]), multiLine=True)  # sc or spark.sparkContext
    # myDF.show(truncate=False)
    myDF.printSchema()
