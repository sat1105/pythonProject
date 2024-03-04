import openai
from pyspark.sql import SparkSession
import pandas as pd

# Set up OpenAI API credentials
openai.api_key = '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'

# Create a SparkSession
spark = SparkSession.builder \
    .appName("OpenAIExample") \
    .getOrCreate()

# Read data from a CSV file
df = spark.read.csv("file:///D:/DATASETS/txs1.csv", header=True, inferSchema=True)

# Perform data preprocessing using PySpark transformations

# Select a column as the prompt for text generation
prompt_df = df.select("product").distinct()

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = prompt_df.toPandas()

# Generate text using OpenAI API for each row
generated_texts = []
for row in pandas_df.itertuples():
    prompt = getattr(row, "product")

    # Generate text using OpenAI GPT-3.5 model
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        max_tokens=100
    )

    generated_text = response.choices[0].text.strip()
    generated_texts.append(generated_text)

# Create a new column with the generated texts
pandas_df["generated_text"] = generated_texts

# Convert Pandas DataFrame back to PySpark DataFrame
generated_df = spark.createDataFrame(pandas_df)

# Perform further analysis or write the generated data to a file, database, etc.
generated_df.show()

# Stop the SparkSession
spark.stop()
