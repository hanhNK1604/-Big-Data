from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat_ws
from pyspark.sql.types import ArrayType, StringType
from underthesea import word_tokenize

spark = SparkSession.builder \
    .appName("Vietnamese Tokenizer") \
    .getOrCreate()

input_file_path = "/home/long/mainUI/Big_Data/python_code/Tokenizer/kieu.txt"
output_path = "/home/long/mainUI/Big_Data/python_code/Tokenizer/output"

df = spark.read.text(input_file_path)
def tokenize_vietnamese(text):
    return word_tokenize(text, format = 'text')

tokenize_udf = udf(tokenize_vietnamese, StringType())
df_tokenized = df.withColumn("tokens", tokenize_udf("value"))

print("Tokenized DataFrame:")
df_tokenized.show(truncate=False)

df_combined = df_tokenized.select(concat_ws(" ", "tokens").alias("value"))
df_combined.write.mode("overwrite").text(output_path)

spark.stop()