from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("read write Hive Managed Table") \
        .config("hive.metastore.uris","thrift://localhost:9083/") \
        .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("use cohort_c8")
    spark.sql('show tables').show()

    spark.sql("""create table if not exists cohort_c8.hv_txn 
         (txnno int, txndate string, custno int, amount double,category string, 
         product string, city string, state string, spendby string)
         row format delimited fields terminated by ',' 
         tblproperties("skip.header.line.count"="1")""")

    spark.sql("load data local inpath '/home/saif/LFS/datasets/txns' into table cohort_c8.hv_txn")

    spark.sql("select count(*) from cohort_c8.hv_txn").show(truncate=False)

    fil_state = spark.sql("select * from cohort_c8.hv_txn where state = 'California'")
    fil_state.show()

    fil_state.write.mode("overwrite").format("parquet").saveAsTable("cohort_c8.fil_txn_state")
    print("***Data written to Hive DB Successfully***")