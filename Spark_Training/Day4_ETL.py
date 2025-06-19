# Databricks notebook source
spark

# COMMAND ----------

user_data = ([(1,'Sristi') ,(2, 'Kush')])
user_schema = "id int, name string"

# COMMAND ----------

df = spark.createDataFrame(data = user_data,schema = user_schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.show()

# COMMAND ----------

df.select("id").display()

# COMMAND ----------

df.select("id", "name").display()

# COMMAND ----------

df.select("id".alias("emp_id"))

# COMMAND ----------

df.select(col("id").alias("emp_id"))

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df1 = df.select(col("id").alias("emp_id"))

# COMMAND ----------

df1.display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id", "name":"emp_name"}).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

input_path = "dbfs:/FileStore/eclerx_inputs/circuits.csv"
df = spark.read.csv(input_path, header = True, inferSchema = True)

# COMMAND ----------

df.display()

# COMMAND ----------

### 1 - rename circuitID ---circuit_id, circuitRef - circuit_ref
### 2 - add new column with current_date
### 3- drop url col

# COMMAND ----------

#df.withColumnsRenamed({"circuitID":"circuit_id","circuitRef":"circuit_ref"}).display()
df1 = df.withColumnsRenamed({"circuitID":"circuit_id","circuitRef":"circuit_ref"})

# COMMAND ----------

#df.withColumn("Current_Date",current_date()).display()
df2 = df.withColumn("Current_Date",current_date())

# COMMAND ----------

df3 = df.drop('URL')

# COMMAND ----------

df3.display()

# COMMAND ----------

df_Final=df.withColumnsRenamed({"circuitID":"circuit_id","circuitref":"circuit_ref"}).withColumn("Ingestion_Date", current_date()).drop("URL")

# COMMAND ----------

df_Final.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists formula1

# COMMAND ----------

df_Final.write.mode('overwrite').saveAsTable("formula1.circuit")

# COMMAND ----------


