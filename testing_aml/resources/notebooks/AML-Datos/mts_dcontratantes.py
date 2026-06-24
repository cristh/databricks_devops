# Databricks notebook source
# DBTITLE 1,-- EXTRACCION CONTRATANTES
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE stp_aml.bronze.mts_contratantes
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(C.contratanteid AS INT)  AS CONTRATANTEID,
# MAGIC   C.contratantecd               AS CONTRATANTECD,
# MAGIC   CAST(C.tipopersonafiscalid AS INT) AS CVE_TIPO_PERSONA,
# MAGIC   P.nombre_completo             AS NOMBRE_COMPLETO,
# MAGIC   P.cve_giro                    AS CVE_GIRO
# MAGIC FROM pg_migracion_read.sofom.mts_dcontratante C
# MAGIC JOIN pg_migracion_read.sofom.mts_kyc_contrato_personas CP
# MAGIC   ON CP.cve_persona = C.contratantecd
# MAGIC  AND CP.cve_rol = 'CL'
# MAGIC JOIN pg_migracion_read.sofom.mts_kyc_personas P
# MAGIC   ON P.id_kyc = CP.id_kyc
# MAGIC JOIN pg_migracion_read.sofom.mts_dtipopersonafiscal F
# MAGIC   ON F.tipopersonafiscalid = C.tipopersonafiscalid
# MAGIC WHERE C.tipopersonafiscalid IN (1, 2, 104)
# MAGIC   AND C.fec_creacion >= DATE '2025-01-01';
