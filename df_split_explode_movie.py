# spark-submit df_split_explode_movie.py

# spark-submit \
# --master yarn \
# --deploy-mode client \
# --driver-memory 1g \
# --executor-memory 2g \
# --num-executors 1 \
# --executor-cores 4 \
# /home/saif/PycharmProjects/cohort_c8/df_split_explode_movie.py


from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructField,StructType,ArrayType
from pyspark.sql.functions import col,split,explode
from pyspark.sql.functions import array_contains

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Where filter") \
        .master('local[*]') \
        .getOrCreate()

    csv_df = spark.read.option("header","true")\
            .option("inferschema","true")\
            .csv("file:///home/saif/LFS/datasets/movies.csv")

    df1 = csv_df.select("movieId","title",explode(split(col("genres"), "\\|")))
    df2 = df1.filter(df1.col == "Comedy")

    df2.write.mode("overwrite").format("csv").save("hdfs://localhost:9000/user/saif/HFS/Input/movie")

    # df2.show()
    df1.printSchema()


# spark-submit \
# --packages com.amazonaws:aws-java-sdk-s3:1.11.874,org.apache.hadoop:hadoop-aws:3.2.0 \
# --master yarn \
# --deploy-mode client \
# --driver-memory 1g \
# --executor-memory 2g \
# --num-executors 1 \
# --executor-cores 4 \
# /home/saif/PycharmProjects/cohort_c8/df_split_explode_movie.py \
# file:///home/saif/LFS/datasets/movies.csv