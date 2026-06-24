# Databricks notebook source
# DBTITLE 1,Migración MTS_HOPERACIONESCNTR_VALIDOS
# MAGIC %md
# MAGIC ## Migración: PostgreSQL → Delta Table
# MAGIC
# MAGIC ### Objetivo
# MAGIC Migrar `"SOFOM"."MTS_HOPERACIONESCNTR_VALIDOS"` desde PostgreSQL a Delta Table con tracking de progreso.
# MAGIC
# MAGIC ### Datos
# MAGIC * **Origen:** PostgreSQL - `"SOFOM"."MTS_HOPERACIONESCNTR_VALIDOS"`
# MAGIC * **Destino:** Delta Table - `stp_aml.bronze.mts_hoperacionescntr_validos`
# MAGIC * **Método:** JDBC con particionamiento paralelo por fecha
# MAGIC * **Estrategia:** Basada en migración exitosa anterior (13 min para 13M registros)
# MAGIC
# MAGIC ### Pasos
# MAGIC 1. **Obtener estadísticas** de la tabla para configurar particionamiento
# MAGIC 2. **Migrar con tracking** de progreso y métricas de rendimiento
# MAGIC 3. **Validar** que los datos se migraron correctamente
# MAGIC 4. **Ver estadísticas** de la tabla Delta creada
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Nota:** Esta configuración ya fue probada exitosamente en la tabla anterior.

# COMMAND ----------

# DBTITLE 1,Por qué particionar por FECHA
# MAGIC %md
# MAGIC ## Estrategia de Particionamiento: FECHAOPERACIONCNTR
# MAGIC
# MAGIC ### Por qué usamos FECHA:
# MAGIC
# MAGIC **Ventajas comprobadas:**
# MAGIC * **Distribución real**: Datos concentrados en períodos específicos
# MAGIC * **Cargas incrementales**: Fácil filtrar datos nuevos por fecha en Jobs futuros
# MAGIC * **Queries optimizados**: Dashboards filtran por fecha, no por ID
# MAGIC * **Data skipping**: Delta Lake optimiza queries temporales
# MAGIC * **Lógica de negocio**: Las operaciones se analizan por fecha
# MAGIC
# MAGIC ### Implementación Técnica:
# MAGIC
# MAGIC Spark JDBC requiere columnas **numéricas** para `numPartitions`.
# MAGIC
# MAGIC **Solución:** Convertir timestamp a epoch (segundos desde 1970):
# MAGIC ```sql
# MAGIC EXTRACT(EPOCH FROM "FECHAOPERACIONCNTR")::BIGINT
# MAGIC ```
# MAGIC
# MAGIC Esto permite:
# MAGIC * Particionar datos por rango temporal
# MAGIC * Distribución uniforme entre 24 particiones
# MAGIC * Lectura paralela eficiente desde PostgreSQL
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Configuración de conexión PostgreSQL
# Configuración de conexión PostgreSQL
jdbc_url = "jdbc:postgresql://dbcluster-postgres-instance-1.cveqsqiyyra8.us-east-2.rds.amazonaws.com:5432/stp_uatpg"

properties = {
    "user": "postgres",
    "password": "4ML_4DM1N",
    "driver": "org.postgresql.Driver",
    "fetchSize": "50000"  # Batch size optimizado
}

# Tabla origen y destino
source_table = '"SOFOM"."MTS_HOPERACIONESCNTR_VALIDOS"'
destination_table = "stp_aml.bronze.mts_hoperacionescntr_validos"

print("Configuración lista")
print(f"Origen: {source_table}")
print(f"Destino: {destination_table}")

# COMMAND ----------

# DBTITLE 1,Paso 1: Obtener estadísticas de la tabla
# Obtener estadísticas de la tabla para configurar particionamiento
print("Obteniendo estadísticas de la tabla...")

# Query para obtener rangos de FECHAOPERACIONCNTR
stats_query = '''
    SELECT 
        MIN("FECHAOPERACIONCNTR") as fecha_min,
        MAX("FECHAOPERACIONCNTR") as fecha_max,
        EXTRACT(EPOCH FROM MIN("FECHAOPERACIONCNTR"))::BIGINT as min_epoch,
        EXTRACT(EPOCH FROM MAX("FECHAOPERACIONCNTR"))::BIGINT as max_epoch,
        COUNT(*) as total_rows
    FROM "SOFOM"."MTS_HOPERACIONESCNTR_VALIDOS"
'''

stats_df = spark.read.jdbc(
    url=jdbc_url,
    table=f"({stats_query}) as stats",
    properties=properties
)

stats = stats_df.collect()[0]

print(f"\nEstadísticas de la tabla:")
print(f"  Total registros: {stats['total_rows']:,}")
print(f"  Rango de fechas: {stats['fecha_min']} a {stats['fecha_max']}")
print(f"  Rango epoch (para particionamiento): {stats['min_epoch']:,} a {stats['max_epoch']:,}")

# Guardar para usar en siguiente celda
min_epoch = int(stats['min_epoch'])
max_epoch = int(stats['max_epoch'])
total_rows = stats['total_rows']

print(f"\nParticionaremos por FECHAOPERACIONCNTR (convertida a epoch)")
print(f"Estrategia probada exitosamente en tabla anterior")

# COMMAND ----------

# DBTITLE 1,Paso 2: Migración con Tracking de Progreso
import time
from datetime import datetime

print("\nIniciando migración con tracking de progreso...\n")
start_time = time.time()

# Configuración de paralelización (misma que funcionó exitosamente)
num_partitions = 24  # Óptimo para serverless

print(f"Configuración:")
print(f"  - Particiones: {num_partitions}")
print(f"  - Columna de partición: FECHAOPERACIONCNTR (como epoch)")
print(f"  - Rango epoch: {min_epoch:,} a {max_epoch:,}")
print(f"  - FetchSize: {properties['fetchSize']}\n")

# Crear subquery con columna epoch para particionamiento
subquery = f'''
    (SELECT 
        *,
        EXTRACT(EPOCH FROM "FECHAOPERACIONCNTR")::BIGINT as fecha_epoch
    FROM "SOFOM"."MTS_HOPERACIONESCNTR_VALIDOS") as data_with_epoch
'''

# Leer datos con particionamiento por fecha
print("Leyendo datos desde PostgreSQL...")
read_start = time.time()

df = spark.read.jdbc(
    url=jdbc_url,
    table=subquery,
    properties=properties,
    numPartitions=num_partitions,
    column="fecha_epoch",  # Columna de fecha convertida a número
    lowerBound=min_epoch,
    upperBound=max_epoch
)

# Eliminar columna auxiliar fecha_epoch
df = df.drop("fecha_epoch")

# Cache para evitar re-lectura
df.cache()

# Contar registros leídos
rows_read = df.count()
read_time = time.time() - read_start

print(f"Lectura completada:")
print(f"  - Registros leídos: {rows_read:,}")
print(f"  - Tiempo: {read_time:.2f} segundos")
print(f"  - Velocidad: {rows_read/read_time:,.0f} registros/seg\n")

# Escribir a Delta Table
print(f"Escribiendo a Delta Table: {destination_table}...")
write_start = time.time()

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("dataChange", "true") \
    .saveAsTable(destination_table)

write_time = time.time() - write_start
total_time = time.time() - start_time

print(f"\nMigración completada exitosamente!\n")
print(f"Resumen:")
print(f"  - Registros migrados: {rows_read:,}")
print(f"  - Tiempo total: {total_time:.2f} segundos ({total_time/60:.2f} minutos)")
print(f"  - Tiempo lectura: {read_time:.2f} seg")
print(f"  - Tiempo escritura: {write_time:.2f} seg")
print(f"  - Velocidad promedio: {rows_read/total_time:,.0f} registros/seg")
print(f"\nTabla creada: {destination_table}")

# COMMAND ----------

# DBTITLE 1,Paso 3: Validación de la migración
# Validar que los datos se migraron correctamente
print("\nValidando migración...\n")

# Leer la tabla Delta creada
delta_df = spark.table(destination_table)

# Contar registros
rows_in_delta = delta_df.count()

print(f"Comparación:")
print(f"  Origen (PostgreSQL): {rows_read:,} registros")
print(f"  Destino (Delta Table): {rows_in_delta:,} registros")

if rows_in_delta == rows_read:
    print(f"\nValidación exitosa! Todos los registros se migraron correctamente.")
else:
    print(f"\nAdvertencia: Diferencia de {abs(rows_in_delta - rows_read):,} registros")

# Mostrar muestra de datos
print(f"\nMuestra de 5 registros:")
display(delta_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Estadísticas de la tabla Delta
# Ver información detallada de la tabla Delta
print(f"\nInformación de la tabla: {destination_table}\n")

# Descripción de la tabla
spark.sql(f"DESCRIBE EXTENDED {destination_table}").show(50, truncate=False)
