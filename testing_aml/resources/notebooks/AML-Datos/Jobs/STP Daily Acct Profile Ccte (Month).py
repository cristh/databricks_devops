# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql.functions import add_months, current_date

# Generar datos nuevos del mes anterior
df_new = spark.sql(f"""
select cast(a.FECHAOPERACIONCNTR as date) AS FECHA, a.CONTRATANTEID, COUNT(*) NUM_TX_TOTAL, SUM(a.MONTOCNTR) as MONTO_TX_TOTAL,
COUNT(CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN a.CUENTA_ORDENANTE END) as NUM_TX_ORDENA, 
SUM(COALESCE(CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN a.MONTOCNTR END, 0)) as MONTO_TX_ORDENA,
CASE WHEN COUNT(DISTINCT CASE WHEN  a.CVE_TIPO_ORDEN = 'E' THEN b.SUFIJO END) = 0 THEN 1 ELSE COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN b.SUFIJO END) END as SUFIJOS_ORDENA,
CASE WHEN COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN b.NUM_CUENTA END) = 0 THEN 1 ELSE COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN b.NUM_CUENTA END) END as DIST_CUENTA_ORDENA,
COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN a.CUENTA_BENEFICIARIO END) AS NUM_BENEF_UNICOS,
CASE WHEN COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN a.CUENTA_BENEFICIARIO END) = 0 THEN 0 ELSE 
    SUM(CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN a.MONTOCNTR END) / COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN a.CUENTA_BENEFICIARIO END) END AS MONTO_PROM_ORDENA,
 COALESCE(
 COUNT(CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN a.CUENTA_ORDENANTE END) / 
 NULLIF(COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'E' THEN a.CUENTA_BENEFICIARIO END), 0),
 0
 ) AS NUM_TX_PROM_X_ORDENA,
 COUNT(CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN a.CUENTA_BENEFICIARIO END) AS NUM_TX_BENEF,
 COALESCE(
 SUM(CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN a.MONTOCNTR END), 
 0
 ) AS MONTO_TX_BENEF,
 CASE WHEN COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN c.SUFIJO END) = 0 THEN 1 ELSE COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN c.SUFIJO END) END as SUFIJOS_BENEF,
  CASE WHEN COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN b.NUM_CUENTA END) = 0 THEN 1 ELSE COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN b.NUM_CUENTA END) END as DIST_CUENTA_BENEF,
 COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN a.CUENTA_ORDENANTE END) AS NUM_ORDENA_UNICOS,
 COALESCE(
 COUNT(CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN a.CUENTA_BENEFICIARIO END) / 
 NULLIF(COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN a.CUENTA_ORDENANTE END), 0), 
 0
 ) AS NUM_TX_PROM_X_BENEF,
 COALESCE(
 SUM(CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN a.MONTOCNTR END) / 
 NULLIF(COUNT(DISTINCT CASE WHEN a.CVE_TIPO_ORDEN = 'R' THEN a.CUENTA_ORDENANTE END), 0),
 0
 ) AS MONTO_PROM_BENEF,
 CASE WHEN d.CVE_TIPO_PERSONA = 1 THEN 'Física' WHEN d.CVE_TIPO_PERSONA = 2 THEN 'Moral' when d.CVE_TIPO_PERSONA = 104 then 'Moral' END AS TIPO_CTE,
 min(f.CVE_NIVEL_RIESGO) AS RIESGO_DECLARADO, 
 min(datediff(month, e.FECHA_ALTA, current_date())) ANTIGUEDAD_MESES,
 PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY HOUR(a.FECHAOPERACIONCNTR)) AS HORA_MEDIAN,
 SUM(CASE WHEN EXTRACT(WEEK FROM a.FECHAOPERACIONCNTR) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', a.FECHAOPERACIONCNTR)) + 1 = 1 THEN 1 ELSE 0 END) AS NUM_TX_SEMANA1, 
 SUM(CASE WHEN EXTRACT(WEEK FROM a.FECHAOPERACIONCNTR) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', a.FECHAOPERACIONCNTR)) + 1 = 2 THEN 1 ELSE 0 END) AS NUM_TX_SEMANA2,
 SUM(CASE WHEN EXTRACT(WEEK FROM a.FECHAOPERACIONCNTR) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', a.FECHAOPERACIONCNTR)) + 1 = 3 THEN 1 ELSE 0 END) AS NUM_TX_SEMANA3,
 SUM(CASE WHEN EXTRACT(WEEK FROM a.FECHAOPERACIONCNTR) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', a.FECHAOPERACIONCNTR)) + 1 = 4 THEN 1 ELSE 0 END) AS NUM_TX_SEMANA4,
 SUM(CASE WHEN EXTRACT(WEEK FROM a.FECHAOPERACIONCNTR) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', a.FECHAOPERACIONCNTR)) + 1 = 5 THEN 1 ELSE 0 END) AS NUM_TX_SEMANA5
FROM stp_aml.bronze.mts_hoperacionescntr a
left join stp_aml.bronze.mts_ds_clientes b on b.NUM_CUENTA = a.CUENTA_ORDENANTE
left join stp_aml.bronze.mts_ds_clientes c on c.NUM_CUENTA = a.CUENTA_BENEFICIARIO
join stp_aml.bronze.mts_contratantes d on a.CONTRATANTEID = d.CONTRATANTEID and d.CVE_TIPO_PERSONA in (1,2,104)
left join stp_aml.bronze.mts_contratantes_tiempo e on e.CONTRATANTEID = d.CONTRATANTEID
left join (select CONTRATANTEID, max(CVE_NIVEL_RIESGO) CVE_NIVEL_RIESGO
        from stp_aml.bronze.mts_perfil_declarado
      group by CONTRATANTEID) f on f.CONTRATANTEID = a.CONTRATANTEID
LEFT JOIN  meltsan_aml.aml_stp_reportes.brz_whitelist_prefijos g on g.PREFIJO = a.PREFIJO
WHERE date_format(CAST(a.FECHAOPERACIONCNTR AS DATE), 'yyyyMM') =  date_format(add_months(current_date(), -1), "yyyyMM")
AND g.PREFIJO is null
AND a.NO_CONSIDERADA = 1
GROUP BY cast(a.FECHAOPERACIONCNTR as date), a.CONTRATANTEID, CASE WHEN d.CVE_TIPO_PERSONA = 1 then 'Física' when d.CVE_TIPO_PERSONA = 2 then 'Moral' when d.CVE_TIPO_PERSONA = 104 then 'Moral' end
""")

# Verificar si la tabla de destino existe
try:
    df_existing = spark.table("stp_aml.silver.acct_profile_daily_ccte")
    table_exists = True
    print("Tabla existente encontrada. Haciendo append incremental...")
except:
    table_exists = False
    print("Primera carga: creando tabla stp_aml.silver.acct_profile_daily_ccte...")

# Procesar según si la tabla existe o no
if table_exists:
    # Tabla existe: hacer left_anti join para evitar duplicados
    df_to_append = df_new.join(df_existing, on=['FECHA','CONTRATANTEID', 'TIPO_CTE'], how="left_anti")
    rows_to_append = df_to_append.count()
    print(f"Registros nuevos a agregar: {rows_to_append:,}")
    
    if rows_to_append > 0:
        df_to_append.write.mode("append").saveAsTable("stp_aml.silver.acct_profile_daily_ccte")
        print(f"Append completado: {rows_to_append:,} registros agregados")
    else:
        print("No hay registros nuevos para agregar")
else:
    # Primera carga: crear la tabla directamente
    rows_to_create = df_new.count()
    print(f"Creando tabla con {rows_to_create:,} registros iniciales...")
    df_new.write.mode("overwrite").saveAsTable("stp_aml.silver.acct_profile_daily_ccte")
    print(f"Tabla creada exitosamente con {rows_to_create:,} registros")
