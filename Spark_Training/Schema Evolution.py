# Databricks notebook source
data = [(1, 'Sristi', 'Mumbai'),(2,'Kairav','Pune')]
Schema = "id int, name string, Place string"

df = spark.createDataFrame(data = data, schema = Schema)

# COMMAND ----------

df.write.saveAsTable('Emp_New')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Emp_New

# COMMAND ----------


