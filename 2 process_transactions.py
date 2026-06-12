from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg

# Создание Spark-сессии с поддержкой Hive
spark = SparkSession.builder \
    .appName("process-transactions") \
    .enableHiveSupport() \
    .getOrCreate()

# Путь CSV-файлу в бакете
input_path = "s3a://etlhw/data/bank_fraud.csv"

# Путь для сохранения результата
output_path = "s3a://etlhw/output/transactions_aggregated"

# Читаем CSV-файл
df = spark.read.option("header", True).csv(input_path)

# Выводим схему данных и количество строк
print("Схема данных:")
df.printSchema()
print(f"Всего транзакций: {df.count()}")

# считаем количество и сумму мошеннических транзакций по странам
fraud_by_country = df.filter(col("is_fraud") == 1) \
    .groupBy("country") \
    .agg(
        count("*").alias("fraud_count"),
        spark_sum("transaction_amount").alias("total_fraud_amount")
    ) \
    .orderBy(col("total_fraud_amount").desc())

print("Мошеннические транзакции по странам:")
fraud_by_country.show(10)

# Сохраняем результат в паркет (эффективный формат)
fraud_by_country.write.mode("overwrite").parquet(output_path)

print(f"Результат сохранен в: {output_path}")

# Останавливаем Spark-сессию
spark.stop()
