# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC NUM_CUENTA,
# MAGIC SUBPREFIJO as SUFIJO,
# MAGIC PREFIJO
# MAGIC FROM pg_migracion_read.sofom.mts_ds_clientes

# COMMAND ----------

_sqldf.write.format("delta").mode("overwrite").saveAsTable("stp_aml.bronze.mts_ds_clientes")
