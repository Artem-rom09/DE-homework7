from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, count
from pyspark.sql.types import IntegerType

# --- Конфігурація ---
KEYSPACE = "amazon_reviews"
SOURCE_DATA_FILE = "amazon_reviews.csv"

def write_to_cassandra(df, table_name):
    """Допоміжна функція для запису DataFrame в Cassandra."""
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .options(table=table_name, keyspace=KEYSPACE) \
      .mode("append") \
      .save()
    print(f"Data successfully written to {KEYSPACE}.{table_name}")

def main():
    spark = SparkSession.builder \
        .appName("AmazonReviewsETL") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .getOrCreate()

    print("--- Spark сесія створена ---")

    df = spark.read.csv(SOURCE_DATA_FILE, sep="\t", header=True, inferSchema=False)

    # --- Очищення та трансформація ---
    cleaned_df = df.select(
        "customer_id", "review_id", "product_id", "product_title",
        "star_rating", "review_headline", "review_body", "review_date", "verified_purchase"
    ).dropna()

    cleaned_df = cleaned_df.withColumn("customer_id", col("customer_id").cast(IntegerType())) \
                           .withColumn("star_rating", col("star_rating").cast(IntegerType())) \
                           .withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))

    cleaned_df = cleaned_df.withColumn("period", date_format(col("review_date"), "yyyy-MM"))
    cleaned_df.cache()
    print(f"--- Дані завантажено та очищено. Загальна кількість рядків: {cleaned_df.count()} ---")

    # --- Завантаження даних в таблиці ---
    # 1. Відгуки за продуктом
    reviews_by_product_df = cleaned_df.select("product_id", "review_date", "review_id", "customer_id", "star_rating", "review_headline", "review_body")
    write_to_cassandra(reviews_by_product_df, "reviews_by_product")

    # 2. Відгуки за продуктом та рейтингом
    reviews_by_product_rating_df = cleaned_df.select("product_id", "star_rating", "review_date", "review_id", "customer_id", "review_headline", "review_body")
    write_to_cassandra(reviews_by_product_rating_df, "reviews_by_product_and_rating")

    # 3. Відгуки за клієнтом
    reviews_by_customer_df = cleaned_df.select("customer_id", "review_date", "review_id", "product_id", "star_rating", "review_headline", "review_body")
    write_to_cassandra(reviews_by_customer_df, "reviews_by_customer")

    # --- Агрегації ---
    # 4. Найпопулярніші продукти
    most_reviewed = cleaned_df.groupBy("period", "product_id", "product_title").agg(count("*").alias("total_reviews"))
    write_to_cassandra(most_reviewed, "most_reviewed_products_by_month")

    # 5. Найпродуктивніші клієнти (тільки підтверджені покупки)
    verified_purchases_df = cleaned_df.filter(col("verified_purchase") == "Y")
    most_productive = verified_purchases_df.groupBy("period", "customer_id").agg(count("*").alias("total_reviews"))
    write_to_cassandra(most_productive, "most_productive_customers_by_month")

    # 6. "Хейтери"
    haters = cleaned_df.filter(col("star_rating").isin([1, 2])).groupBy("period", "customer_id").agg(count("*").alias("hater_reviews"))
    write_to_cassandra(haters, "haters_by_month")

    # 7. "Прихильники"
    backers = cleaned_df.filter(col("star_rating").isin([4, 5])).groupBy("period", "customer_id").agg(count("*").alias("backer_reviews"))
    write_to_cassandra(backers, "backers_by_month")

    cleaned_df.unpersist()
    spark.stop()
    print("--- Роботу завершено ---")

if __name__ == "__main__":
    main()
