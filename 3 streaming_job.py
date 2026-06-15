from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# подключения
KAFKA_BROKER = "rc1a-fhvkfepi1uj85ud1.mdb.yandexcloud.net:9091"
KAFKA_USER = "user"
KAFKA_PASSWORD = "hgfdsa12"

PG_HOST = "rc1b-kq1fpec9drltqmvi.mdb.yandexcloud.net"
PG_DB = "loan_analytics"
PG_USER = "datalens_user"
PG_PASSWORD = "hgfdsa12"

# Схема JSON
document_schema = StructType([
    StructField("type", StringType(), True),
    StructField("status", StringType(), True)
])

input_schema = StructType([
    StructField("application_id", StringType(), True),
    StructField("customer", StructType([
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True)
    ])),
    StructField("loan", StructType([
        StructField("amount", IntegerType(), True),
        StructField("term_months", IntegerType(), True)
    ])),
    StructField("scoring", StructType([
        StructField("score", IntegerType(), True),
        StructField("risk_level", StringType(), True)
    ])),
    StructField("documents", ArrayType(document_schema), True),
    StructField("decision_status", StringType(), True),
    StructField("submitted_at", StringType(), True)
])

# Создаём Spark сессию
spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

print("Spark сессия создана")

# Читаем поток из Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";') \
    .option("kafka.ssl.truststore.location", "/tmp/CA.pem") \
    .option("subscribe", "loan_applications") \
    .option("startingOffsets", "earliest") \
    .load()

print("Подключение к Kafka настроено")

# Парсим JSON и разворачиваем
parsed = raw_stream \
    .select(from_json(col("value").cast("string"), input_schema).alias("data")) \
    .select(
        col("data.application_id"),
        col("data.customer.customer_id"),
        col("data.customer.region"),
        col("data.loan.amount"),
        col("data.loan.term_months"),
        col("data.scoring.score"),
        col("data.scoring.risk_level"),
        explode(col("data.documents")).alias("doc"),
        col("data.decision_status"),
        to_timestamp(col("data.submitted_at")).alias("submitted_at")
    ) \
    .select("*", col("doc.type").alias("document_type")) \
    .drop("doc")

print(" JSON распарсен и flattened")

# Агрегация по минутам
aggregated = parsed \
    .withWatermark("submitted_at", "10 seconds") \
    .groupBy(
        window(col("submitted_at"), "1 minute"),
        col("decision_status")
    ) \
    .count() \
    .select("window.start", "window.end", "decision_status", "count")

# Функция записи в PostgreSQL
def write_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", f"jdbc:postgresql://{PG_HOST}:5432/{PG_DB}") \
        .option("dbtable", "loan_aggregations") \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .save()
    print(f"Записано {df.count()} строк в PostgreSQL")

# Запускаем стриминг
query = aggregated.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Потоковый джоб запущен. Жду данных из Kafka...")
print("Нажми Ctrl+C для остановки\n")

query.awaitTermination()