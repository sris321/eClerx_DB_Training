-- Databricks notebook source
create table emp(id int, name string, city string)

-- COMMAND ----------

desc extended emp

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000000.json

-- COMMAND ----------

desc history emp

-- COMMAND ----------

insert into emp values(1, 'Sristi','Mumbai')

-- COMMAND ----------

select * from emp

-- COMMAND ----------

desc detail emp

-- COMMAND ----------

desc history emp

-- COMMAND ----------

insert into emp values(2, 'Gaurav', 'Pune'),(3, 'Karan', 'Chandigarh'),(4,'Kairav','Mumbai')

-- COMMAND ----------

insert into emp values(5, 'Himanshu','Mumbai');
insert into emp values(5, 'Sikha','Pune');

-- COMMAND ----------

select * from emp

-- COMMAND ----------

update emp
set id = 6 where name = 'Sikha'

-- COMMAND ----------

desc detail emp

-- COMMAND ----------

desc history emp

-- COMMAND ----------

delete from emp where id = 1

-- COMMAND ----------

select * from emp version as of 4

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select * from emp timestamp as of '2025-06-09T07:04:09.000+00:00'

-- COMMAND ----------


