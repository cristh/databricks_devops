# Databricks notebook source
# %sql
# SELECT
#   CAST(C.contratanteid AS INT) AS contratanteid,
#   C.contratantecd,
#   CAST(C.fec_creacion AS DATE) AS fecha_alta
# FROM pg_migracion_read.sofom.mts_dcontratante C
# JOIN pg_migracion_read.sofom.mts_dtipopersonafiscal F
#   ON F.tipopersonafiscalid = C.tipopersonafiscalid
# WHERE C.tipopersonafiscalid IN (1, 2, 104)
#   AND C.fec_creacion >= DATE '2020-01-01'
# LIMIT 100;

# COMMAND ----------

# DBTITLE 1,FECHA_ALTA
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE stp_aml.bronze.mts_contratantes_tiempo
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(C.contratanteid AS INT) AS CONTRATANTEID,
# MAGIC   C.contratantecd               AS CONTRATANTECD,
# MAGIC   CAST(C.fec_creacion AS DATE)  AS FECHA_ALTA
# MAGIC FROM pg_migracion_read.sofom.mts_dcontratante C
# MAGIC JOIN pg_migracion_read.sofom.mts_dtipopersonafiscal F
# MAGIC   ON F.tipopersonafiscalid = C.tipopersonafiscalid
# MAGIC WHERE C.tipopersonafiscalid IN (1, 2, 104)
# MAGIC   AND C.fec_creacion >= DATE '2020-01-01';
