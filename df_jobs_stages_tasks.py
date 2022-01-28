from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder\
        .appName('Job stage Tasks')\
        .master('local[*]')\
        .appName("My first program")\
        .getOrCreate()
# for inferschema 2 jobs are created

    medicalDf = spark.read.format('csv')\
        .option('header','true')\
        .option('inferSchema','true')\
        .load('file:///home/saif/LFS/datasets/medicalData.csv')
    partition_df = medicalDf.repartition(2)

    filter_df  = partition_df.where("Age<40")
    select_df = filter_df.select("Age","Gender","Country","State")
    group_df = select_df.groupBy("Country")
    count_df = group_df.count()

    count_df.collect()
    input('Dummy')

















