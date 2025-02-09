# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists inflation_bronze.inflation_annual; 
# MAGIC

# COMMAND ----------

df_ccpi= spark.read.format("delta").table("inflation_bronze.inflation_ccpi_annual").drop("Note")
df_def= spark.read.format("delta").table("inflation_bronze.inflation_def_annual")
df_ecpi= spark.read.format("delta").table("inflation_bronze.inflation_ecpi_annual").drop("Note")
df_fcpi= spark.read.format("delta").table("inflation_bronze.inflation_fcpi_annual").drop("Note")
df_hcpi= spark.read.format("delta").table("inflation_bronze.inflation_hcpi_annual").drop("Note")
df_ppi= spark.read.format("delta").table("inflation_bronze.inflation_ppi_annual").drop("Note")

# COMMAND ----------

df_merged = df_ccpi.union(df_def).union(df_ecpi).union(df_fcpi).union(df_hcpi).union(df_ppi)


# COMMAND ----------

df_remove_null = df_merged.filter("Country_Code is not null").drop("IMF_Country_Code")

# COMMAND ----------

df_filter_note = df_remove_null.filter(~df_remove_null["Country_Code"].like("%Note%"))


# COMMAND ----------

df_transform = df_filter_note.fillna(0)

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table inflation_silver.inflation_annual_transformed

# COMMAND ----------


df_transform.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("inflation_silver.inflation_annual_transformed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inflation_silver.inflation_annual_transformed

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY inflation_silver.inflation_annual_transformed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM inflation_silver.inflation_annual_transformed VERSION AS OF 5 limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE inflation_silver.inflation_annual_transformed
# MAGIC ZORDER BY (Country_Code)

# COMMAND ----------

distinct_values_list = [row.Country_Code for row in df_transform.select("Country_Code").distinct().collect()]
print(distinct_values_list)

# COMMAND ----------

dbutils.widgets.dropdown("dropdown_filter", "IND", choices=distinct_values_list, label='Dropdown')

# COMMAND ----------

from pyspark.sql.functions import col
display(df_transform.filter(col('Country_Code')==dbutils.widgets.get('dropdown_filter')))