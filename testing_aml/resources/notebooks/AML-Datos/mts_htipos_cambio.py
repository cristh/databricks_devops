# Databricks notebook source
# =========================
# 1) Lectura desde Lakehouse Federation
# =========================

df = spark.table(
    "pg_migracion_read.sofom.mts_htipos_cambio"
)

# =========================
# 2) Escritura a Delta (overwrite)
# =========================

df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("stp_aml.bronze.mts_tipos_cambio")
