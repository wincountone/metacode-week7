from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_PATH = "/opt/spark/work-dir/data/wordcount.txt"


def main():
    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    lines = spark.read.text(DATA_PATH)
    words = (
        lines.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
        .filter(F.col("word") != "")
    )

    total_words = words.count()
    counts = words.groupBy("word").count()
    distinct_words = counts.count()

    print(f"total_words={total_words} distinct_words={distinct_words}")

    counts.orderBy(F.desc("count")).show()

    spark.stop()


if __name__ == "__main__":
    main()
