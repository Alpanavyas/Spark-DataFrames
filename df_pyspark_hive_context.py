from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import HiveContext

if __name__ == '__main__':
    sparkConf = SparkConf()\
        .setAppName('PySpark Hive Intgn')\
        .setMaster('local[*]')
    sc = SparkContext(conf=sparkConf)
    hc = HiveContext(sc)

    hc.sql("use cohort_c8")
    hc.sql('show tables').show()