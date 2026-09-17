import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="local path to netflix_titles.csv")
    parser.add_argument("--output", required=True, help="local path to write the parquet result")
    parser.add_argument("--min-year", type=int, default=2015, help="minimum release_year to keep")
    return parser.parse_args()


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("weekly_pipeline_transform").getOrCreate()

    df = spark.read.csv(args.input, header=True, inferSchema=True, multiLine=True, escape='"')

    recent = df.filter(F.col("release_year") >= args.min_year)

    by_genre = (
        recent.withColumn("genre", F.explode(F.split(F.col("listed_in"), ",")))
        .withColumn("genre", F.trim(F.col("genre")))
    )

    agg = (
        by_genre.groupBy("type", "genre")
        .agg(F.count(F.lit(1)).alias("title_count"))
        .orderBy("type", "genre")
    )

    agg.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(args.output)

    row_count = agg.count()
    agg.show(row_count, truncate=False)
    print(f"aggregated rows = {row_count}")

    spark.stop()


if __name__ == "__main__":
    main()
