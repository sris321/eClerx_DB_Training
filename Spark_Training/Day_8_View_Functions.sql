-- Databricks notebook source
-- MAGIC %fs ls

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df=spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv", header = True, inferSchema = True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.saveAsTable("Bike_day")

-- COMMAND ----------

select * from Bike_day

-- COMMAND ----------

create or replace view max_month as select mnth, round(max(temp),2) as max from Bike_day group by mnth order by max desc

-- COMMAND ----------

desc extended max_month

-- COMMAND ----------

create or replace temp view holiday_count_temp as select mnth, count(*) as count from hive_metastore.default.bike_day where holiday =1 group by mnth

-- COMMAND ----------

show views

-- COMMAND ----------

select * from holiday_count_temp

-- COMMAND ----------

create or replace function fullname(first_name string, last_name string)
returns string
return concat(first_name, " ",last_name)

-- COMMAND ----------

select fullname("Sristi", "Kushwaha") as name

-- COMMAND ----------

create table example(id int, forename string, surname string, age int);
insert into example values(1, 'Akash', 'Yadav', 29),(2, 'Kairav', 'Vish', 30), (3, 'Misha','Gaud',25)

-- COMMAND ----------

select * from example

-- COMMAND ----------

select * , fullname(forename, surname) from example

-- COMMAND ----------

select id, fullname(forename, surname), age from example

-- COMMAND ----------

-- Task:
--udf function

--age>60 senior
--age 20 to 60 adult
--age <20 teenager

-- COMMAND ----------

create or replace function Age_Distribution(age int)
returns string
return 
case when age>60 then "Senior"
when age >=20 and age<=60 then "Adult"
when age<20 then "Teenager"
end

-- COMMAND ----------

select Age_Distribution(40) as Age_Group

-- COMMAND ----------

select Age_Distribution(10) as Age_Group

-- COMMAND ----------


