from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CustomerJob").getOrCreate()

df = spark.read.csv(
    "gs://rajesh-demo-project/raw/customer_transactions.csv",
    header=True,
    inferSchema=True
)

# Simple Transformation
df = df.filter(df.amount > 600)

# Write to BigQuery
df.write.format("bigquery") \
    .option("table", "rajesh-ecp-project.sales_data.customer_transactions") \
    .mode("overwrite") \
    .save()

spark.stop()