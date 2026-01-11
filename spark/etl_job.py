from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("MovieLensETL") \
    .master("local[*]") \
    .getOrCreate()

ratings_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/raw/ml-32m/ratings.csv")

movies_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/raw/ml-32m/movies.csv")

ratings_df.printSchema()
movies_df.printSchema()

ratings_df.show(5)
movies_df.show(5)

ratings_df = ratings_df.dropna(subset=["userId", "movieId", "rating"])
movies_df = movies_df.dropna(subset=["movieId", "title"])

movie_ratings_df = ratings_df.join(
    movies_df,
    on="movieId",
    how="left"
)

from pyspark.sql.functions import from_unixtime

movie_ratings_df = movie_ratings_df.withColumn(
    "rating_timestamp",
    from_unixtime(col("timestamp"))
)

movie_ratings_df.write \
    .mode("overwrite") \
    .parquet("data/processed/movie_ratings")

spark.stop()

