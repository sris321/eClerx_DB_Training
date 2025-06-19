# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/eclerx_inputs")

# COMMAND ----------

# dbutils.fs.rm(Path to remove file)

