-- Databricks notebook source
-- MAGIC %python
-- MAGIC print("Hello")

-- COMMAND ----------

select "Run SQL"

-- COMMAND ----------

 --- Task - Create schema/database, create table without primary keys, insert records, query table

 Create database DataBricks

-- COMMAND ----------

create table hive_metastore.databricks.demo(id int, name string, age int)

-- COMMAND ----------

describe extended hive_metastore.databricks.demo

-- COMMAND ----------


