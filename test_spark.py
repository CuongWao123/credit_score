from pyspark.sql import SparkSession

# Tạo Spark Session - kết nối tới Spark Master cluster
spark = SparkSession.builder \
    .appName("HelloWorld") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

# Print Hello World
print("=" * 50)
print("Hello World from PySpark!")
print("=" * 50)

# Tạo một DataFrame đơn giản
data = [("Hello", 1), ("World", 2), ("from", 3), ("Spark", 4)]
df = spark.createDataFrame(data, ["message", "id"])

print("\nDataFrame:")
df.show()

# Một số thông tin về Spark Session
print(f"\nSpark Version: {spark.version}")
print(f"Application Name: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")

# Dừng Spark Session
spark.stop()
print("\nSpark Session stopped successfully!")