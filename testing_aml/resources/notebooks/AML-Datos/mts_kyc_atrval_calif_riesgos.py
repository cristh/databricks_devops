# Databricks notebook source
# %sql
# SELECT
#   CAST(R.id_kyc AS INT) AS ID_KYC,
#   CAST(C.contratanteid AS INT) AS CONTRATANTEID,
#   C.contratantecd AS CONTRATANTECD,
#   CO.cve_nivel_riesgo AS CVE_NIVEL_RIESGO,
#   CASE
#     WHEN R.cve_atributo IN (
#       'CVE_NUM_OPERACIONES_RECIBIDAS',
#       'CVE_MONTO_OPERACIONES_RECIBIDAS',
#       'CVE_NUM_OPERACIONES_RECIBIDAS_PM',
#       'CVE_MONTO_OPERACIONES_RECIBIDAS_PM',
#       'CVE_NUM_OPERACIONES_RECIBIDAS_FAE',
#       'CVE_MONTO_OPERACIONES_RECIBIDAS_FAE'
#     ) THEN 'R'
#     ELSE 'E'
#   END AS CVE_TIPO_ORDEN,
#   R.cve_atributo AS CVE_ATRIBUTO,
#   R.ds_valor AS VALOR,
#   M.valor_atributo_10 AS VALOR_DECLARADO,
#   M.valor_atributo_05 AS DS_RANGO
# FROM pg_migracion_read.sofom.mts_dcontratante C
# JOIN pg_migracion_read.sofom.mts_kyc_contrato_personas CP
#   ON CP.cve_persona = C.contratantecd
#  AND CP.cve_rol = 'CL'
# JOIN pg_migracion_read.sofom.mts_kyc_contratos CO
#   ON CO.id_contrato = CP.id_contrato
# JOIN pg_migracion_read.sofom.mts_kyc_atrval_calif_riesgos R
#   ON R.id_kyc = CP.id_kyc
# JOIN pg_migracion_read.sofom.mts_ana_dcatalogos_claves M
#   ON M.cve_tabla LIKE '%CAT_MATRIZ_RIESGO_INI%'
#  AND M.clave_01 = R.cve_atributo
#  AND M.vigencia = 'S'
#  AND M.clave_02 = COALESCE(R.ds_valor, '1')
# JOIN pg_migracion_read.sofom.mts_dtipopersonafiscal F
#   ON F.tipopersonafiscalid = C.tipopersonafiscalid
# WHERE C.tipopersonafiscalid IN (1, 2, 104)
#   AND R.cve_atributo IN (
#     'CVE_NUM_OPERACIONES_RECIBIDAS','CVE_MONTO_OPERACIONES_RECIBIDAS','CVE_NUM_OPERACIONES_ENVIADAS','CVE_MONTO_OPERACIONES_ENVIADAS',
#     'CVE_NUM_OPERACIONES_RECIBIDAS_PM','CVE_MONTO_OPERACIONES_RECIBIDAS_PM','CVE_NUM_OPERACIONES_ENVIADAS_PM','CVE_MONTO_OPERACIONES_ENVIADAS_PM',
#     'CVE_NUM_OPERACIONES_RECIBIDAS_FAE','CVE_MONTO_OPERACIONES_RECIBIDAS_FAE','CVE_NUM_OPERACIONES_ENVIADAS_FAE','CVE_MONTO_OPERACIONES_ENVIADAS_FAE'
#   )
#   AND C.fec_creacion >= DATE '2025-01-01'
# LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE stp_aml.bronze.mts_perfil_declarado
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(R.id_kyc AS INT) AS ID_KYC,
# MAGIC   CAST(C.contratanteid AS INT) AS CONTRATANTEID,
# MAGIC   C.contratantecd AS CONTRATANTECD,
# MAGIC   CO.cve_nivel_riesgo AS CVE_NIVEL_RIESGO,
# MAGIC   CASE
# MAGIC     WHEN R.cve_atributo IN (
# MAGIC       'CVE_NUM_OPERACIONES_RECIBIDAS',
# MAGIC       'CVE_MONTO_OPERACIONES_RECIBIDAS',
# MAGIC       'CVE_NUM_OPERACIONES_RECIBIDAS_PM',
# MAGIC       'CVE_MONTO_OPERACIONES_RECIBIDAS_PM',
# MAGIC       'CVE_NUM_OPERACIONES_RECIBIDAS_FAE',
# MAGIC       'CVE_MONTO_OPERACIONES_RECIBIDAS_FAE'
# MAGIC     ) THEN 'R'
# MAGIC     ELSE 'E'
# MAGIC   END AS CVE_TIPO_ORDEN,
# MAGIC   R.cve_atributo AS CVE_ATRIBUTO,
# MAGIC   R.ds_valor AS VALOR,
# MAGIC   M.valor_atributo_10 AS VALOR_DECLARADO,
# MAGIC   M.valor_atributo_05 AS DS_RANGO
# MAGIC FROM pg_migracion_read.sofom.mts_dcontratante C
# MAGIC JOIN pg_migracion_read.sofom.mts_kyc_contrato_personas CP
# MAGIC   ON CP.cve_persona = C.contratantecd
# MAGIC  AND CP.cve_rol = 'CL'
# MAGIC JOIN pg_migracion_read.sofom.mts_kyc_contratos CO
# MAGIC   ON CO.id_contrato = CP.id_contrato
# MAGIC JOIN pg_migracion_read.sofom.mts_kyc_atrval_calif_riesgos R
# MAGIC   ON R.id_kyc = CP.id_kyc
# MAGIC JOIN pg_migracion_read.sofom.mts_ana_dcatalogos_claves M
# MAGIC   ON M.cve_tabla LIKE '%CAT_MATRIZ_RIESGO_INI%'
# MAGIC  AND M.clave_01 = R.cve_atributo
# MAGIC  AND M.vigencia = 'S'
# MAGIC  AND M.clave_02 = COALESCE(R.ds_valor, '1')
# MAGIC JOIN pg_migracion_read.sofom.mts_dtipopersonafiscal F
# MAGIC   ON F.tipopersonafiscalid = C.tipopersonafiscalid
# MAGIC WHERE C.tipopersonafiscalid IN (1, 2, 104)
# MAGIC   AND R.cve_atributo IN (
# MAGIC     'CVE_NUM_OPERACIONES_RECIBIDAS','CVE_MONTO_OPERACIONES_RECIBIDAS','CVE_NUM_OPERACIONES_ENVIADAS','CVE_MONTO_OPERACIONES_ENVIADAS',
# MAGIC     'CVE_NUM_OPERACIONES_RECIBIDAS_PM','CVE_MONTO_OPERACIONES_RECIBIDAS_PM','CVE_NUM_OPERACIONES_ENVIADAS_PM','CVE_MONTO_OPERACIONES_ENVIADAS_PM',
# MAGIC     'CVE_NUM_OPERACIONES_RECIBIDAS_FAE','CVE_MONTO_OPERACIONES_RECIBIDAS_FAE','CVE_NUM_OPERACIONES_ENVIADAS_FAE','CVE_MONTO_OPERACIONES_ENVIADAS_FAE'
# MAGIC   )
# MAGIC   AND C.fec_creacion >= DATE '2025-01-01';

# COMMAND ----------

# MAGIC %md
# MAGIC Hace falta hacer la limpieza de la columna DS_RANGO porque trae texto, y en el actual solo tenemos datos numericos
# MAGIC
