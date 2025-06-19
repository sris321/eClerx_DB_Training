-- Databricks notebook source
create table Delta_New (id int , name string)

-- COMMAND ----------

insert into Delta_New values (1, 'a')

-- COMMAND ----------

insert into Delta_New values (2, 'b');
insert into Delta_New values (3, 'c');
insert into Delta_New values (4, 'd');
insert into Delta_New values (5, 'e');
insert into Delta_New values (6, 'f');
insert into Delta_New values (7, 'g');

-- COMMAND ----------

select * from Delta_New

-- COMMAND ----------

delete from delta_new where id >4

-- COMMAND ----------

select * from delta_new

-- COMMAND ----------

desc extended delta_new

-- COMMAND ----------

desc detail delta_new

-- COMMAND ----------

optimize delta_new
zorder by(id)

-- COMMAND ----------

select * from delta_new

-- COMMAND ----------

delete from delta_new

-- COMMAND ----------

select * from delta_new

-- COMMAND ----------

restore table delta_new to version as of 9

-- COMMAND ----------

select * from delta_new

-- COMMAND ----------

vacuum delta_new

-- COMMAND ----------

vacuum delta_new retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum delta_new retain 0 hours

-- COMMAND ----------


