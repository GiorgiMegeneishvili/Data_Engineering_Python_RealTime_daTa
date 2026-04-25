from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, to_timestamp

# ────────────────────────────────────────────────
# Schema (based on real data)
# ────────────────────────────────────────────────

schema = StructType([
    StructField("crypto",      StringType(),  True),
    StructField("price",       DoubleType(),  True),
    StructField("market_cap",  DoubleType(),  True),
    StructField("24h_volume",  DoubleType(),  True),
    StructField("24h_change",  DoubleType(),  True),
    StructField("last_updated", StringType(), True)   # "2025-03-20 14:35:22" format
])

# ────────────────────────────────────────────────
#                Spark Session
# ────────────────────────────────────────────────
spark = (SparkSession.builder
    .appName("Crypto Kafka → PostgreSQL")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.postgresql:postgresql:42.7.4")
    .config("spark.sql.streaming.checkpointLocation", "/home/giorgi/spark_checkpoints/crypto")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# ────────────────────────────────────────────────
#                  Kafka → DataFrame
# ────────────────────────────────────────────────
kafka_df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "Coin-stream")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load())

value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = value_df.select(
    from_json(col("json_str"), schema).alias("data")
).select("data.*")

# # Time column transformation — this format is compatible with the producer
enriched_df = parsed_df.withColumn(
    "last_updated_ts",
    to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm:ss")
).withColumnRenamed("crypto", "symbol")

# ────────────────────────────────────────────────
#           Write to PostgreSQL (using foreachBatch)
# ────────────────────────────────────────────────
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    # # Column mapping (modify if your table has different column names)
    db_df = batch_df.select(
        col("symbol").alias("symbol"),
        col("price").alias("price_usd"),
        col("market_cap").alias("market_cap"),
        col("24h_volume").alias("volume_24h"),
        col("24h_change").alias("change_24h"),
        col("last_updated_ts").alias("last_updated")
    )

    try: #### Enter here Your DataBase Info
        (db_df.write
            .format("jdbc")
            .option("url",      "jdbc:postgresql://localhost:5432/")
            .option("dbtable",  "crypto_prices")
            .option("user",     "postgres")
            .option("password", "")          # ← უსაფრთხოდ შეინახე .env-ში ან secrets-ში
            .option("driver",   "org.postgresql.Driver")
            .option("stringtype", "unspecified")        # timezone-ის პრობლემების თავიდან ასაცილებლად
            .mode("append")
            .save())

        print(f"Batch {batch_id} წარმატებით ჩაიწერა → {batch_df.count()} ჩანაწერი")

    except Exception as e:
        print(f"Batch {batch_id} ჩაწერის შეცდომა: {str(e)}")

# ────────────────────────────────────────────────
#              Streaming Query
# ────────────────────────────────────────────────
query = (enriched_df
    .writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/home/gm/spark_checkpoints/crypto_postgres")   # ← აქ შეცვალე
    .start())

print("Streaming job started... (Press Ctrl+C to stop)")

query.awaitTermination()
