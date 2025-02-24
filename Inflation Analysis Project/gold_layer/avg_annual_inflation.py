# Databricks notebook source
df_annual = spark.read.format("delta").table("inflation_silver.inflation_annual_transformed")

# COMMAND ----------

from pyspark.sql.functions import *
df_avg = df_annual.groupBy("Country_Code", "Country", "Indicator_Type").agg(avg("2020").alias("2020Avg"), avg("2021").alias("2021Avg"), avg("2023").alias("2023Avg"))

# COMMAND ----------

from pyspark.sql.functions import col

columns_to_round = ["2020Avg", "2021Avg", "2023Avg"] 
df_rounded = df_avg.select(*[round(col(c), 2).alias(c) if c in columns_to_round else col(c) for c in df_avg.columns])

# COMMAND ----------

df_rounded.show()

# COMMAND ----------

df_rounded.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("inflation_gold.inflation_annual_avg")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inflation_gold.inflation_annual_avg order by Country_Code

# COMMAND ----------

distinct_values_list = [row.Country_Code for row in df_rounded.select("Country_Code").distinct().collect()]
print(distinct_values_list)

# COMMAND ----------

dbutils.widgets.dropdown("dropdown_filter_country", "IND", choices=distinct_values_list, label='Dropdown_Aggregate')

# COMMAND ----------

from pyspark.sql.functions import col
display(df_rounded.filter(col('Country_Code')==dbutils.widgets.get('dropdown_filter_country')))