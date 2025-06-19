-- Databricks notebook source
create table Test (id int, name string, age int)

-- COMMAND ----------

insert into Test values (1, "Sristi", 20)

-- COMMAND ----------


describe extended Test

-- COMMAND ----------

select * from test
