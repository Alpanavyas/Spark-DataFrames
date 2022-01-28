from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from urllib.request import urlopen
from pyspark.sql.functions import explode

if __name__ == '__main__':
    sparkConf = SparkConf()
    sc = SparkContext(conf=sparkConf)
    spark = SparkSession.builder\
                .appName('Read webUrl data')\
                .master('local[*]')\
                .getOrCreate()

    readFile = urlopen("https://randomuser.me/api/0.8/?results=10").read().decode("utf-8")
    # print(readFile)
    myDf = spark.read.json(sc.parallelize([readFile]),multiLine=True)
    # myDf.printSchema()
    # myDf.show(truncate=False)

    extract_fields = myDf.withColumn("results",explode("results"))
    extract_fields.printSchema()
    extract_fields.show()

    flattenDf = extract_fields.select("nationality",
                                    "results.user.cell","results.user.dob","results.user.email","results.user.gender",
                                      "results.user.location.city","results.user.location.state","results.user.location.street",
                                      "results.user.location.zip","results.user.md5","results.user.name.first","results.user.name.last",
                                      "results.user.name.title","results.user.password","results.user.phone")
    flattenDf.show(truncate=False)