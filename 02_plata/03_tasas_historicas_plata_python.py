# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 03 — Tasas históricas de UF (Plata)
# MAGIC
# MAGIC La conversión de un precio en UF a CLP necesita el valor de la UF vigente
# MAGIC el día en que se publicó ese aviso específico, no el valor de hoy. Este
# MAGIC notebook identifica qué fechas se necesitan (a partir de los avisos con
# MAGIC moneda UF en Bronce), consulta el histórico de mindicador.cl por año, y
# MAGIC cachea solo las fechas nuevas en `gran_concepcion.02_plata.valores_pesos`.
# MAGIC
# MAGIC El dólar se mantiene como valor fijo aproximado (no varía lo suficiente
# MAGIC día a día como para justificar un histórico completo).
# MAGIC
# MAGIC **Este notebook corre PRIMERO**, antes de la limpieza SQL (04), ya que
# MAGIC esta última necesita las tasas ya cacheadas para convertir precios a CLP.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Importar librerías

# COMMAND ----------

from datetime import date

import pandas as pd
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Crear el esquema de Plata y la tabla de tasas (si no existen)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gran_concepcion.02_plata;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gran_concepcion.02_plata.valores_pesos (
# MAGIC     fecha_valor      DATE,
# MAGIC     valor_uf_clp     DOUBLE,
# MAGIC     valor_dolar_clp  DOUBLE,
# MAGIC     fecha_consulta   DATE
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Identificar qué fechas de UF se necesitan
# MAGIC A partir de los avisos en Bronce con moneda UF, usando la fecha de
# MAGIC publicación aproximada (o `first_seen` como respaldo si no hay fecha).

# COMMAND ----------

fechas_necesarias_rows = spark.sql("""
    SELECT DISTINCT
        COALESCE(d.fecha_publicacion_aprox, a.first_seen) AS fecha_referencia
    FROM gran_concepcion.01_bronce.avisos a
    INNER JOIN gran_concepcion.01_bronce.avisos_detalle d ON a.id_aviso = d.id_aviso
    WHERE lower(a.moneda) = 'uf'
      AND COALESCE(d.fecha_publicacion_aprox, a.first_seen) IS NOT NULL
""").collect()

fechas_necesarias = {row["fecha_referencia"] for row in fechas_necesarias_rows}
print(f"{len(fechas_necesarias)} fechas distintas de UF necesarias.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Descartar fechas que ya están cacheadas
# MAGIC Evita volver a consultar la API para fechas que ya tenemos guardadas.

# COMMAND ----------

try:
    fechas_existentes_rows = spark.sql("""
        SELECT DISTINCT CAST(fecha_valor AS STRING) AS fecha_valor
        FROM gran_concepcion.02_plata.valores_pesos
    """).collect()
    fechas_existentes = {row["fecha_valor"] for row in fechas_existentes_rows}
except Exception:
    fechas_existentes = set()

fechas_faltantes = fechas_necesarias - fechas_existentes
print(f"{len(fechas_faltantes)} fechas todavía no cacheadas, se consultarán a la API.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Consultar mindicador.cl por año
# MAGIC Una sola llamada por año trae la serie completa — mucho más eficiente que
# MAGIC una llamada por fecha individual.

# COMMAND ----------

anios_necesarios = {fecha[:4] for fecha in fechas_faltantes}
valores_uf_por_fecha = {}

for anio in sorted(anios_necesarios):
    print(f"Consultando UF del año {anio}...")
    resp = requests.get(f"https://mindicador.cl/api/uf/{anio}", timeout=15)
    resp.raise_for_status()
    data = resp.json()

    for item in data.get("serie", []):
        fecha_str = item["fecha"][:10]
        valores_uf_por_fecha[fecha_str] = item["valor"]

print(f"{len(valores_uf_por_fecha)} valores de UF obtenidos en total (todos los años consultados).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Armar el DataFrame solo con las fechas que faltaban
# MAGIC El dólar se agrega como valor fijo aproximado en cada fila, junto a la UF
# MAGIC consultada.

# COMMAND ----------

VALOR_DOLAR_CLP = 925.0   # fijo, aproximado - no varía lo suficiente para justificar histórico

registros_nuevos = []
fechas_no_encontradas = []

for fecha in sorted(fechas_faltantes):
    valor = valores_uf_por_fecha.get(fecha)
    if valor is None:
        fechas_no_encontradas.append(fecha)
        continue
    registros_nuevos.append({
        "fecha_valor": fecha,
        "valor_uf_clp": valor,
        "valor_dolar_clp": VALOR_DOLAR_CLP,
        "fecha_consulta": date.today().isoformat(),
    })

if fechas_no_encontradas:
    print(f"{len(fechas_no_encontradas)} fechas no encontradas en la API "
          f"(posiblemente fines de semana/feriados sin publicación oficial): "
          f"{fechas_no_encontradas[:10]}...")

df_uf_nuevos = pd.DataFrame(registros_nuevos)
print(f"{len(df_uf_nuevos)} filas nuevas para insertar")
df_uf_nuevos.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Crear vista temporal
# MAGIC Solo si hay filas nuevas para insertar — si todas las fechas necesarias ya
# MAGIC estaban cacheadas, no hay nada que hacer.

# COMMAND ----------

if len(df_uf_nuevos) > 0:
    spark.createDataFrame(df_uf_nuevos).createOrReplaceTempView("valores_pesos_nuevos_tmp")
else:
    print("No hay filas nuevas para insertar — todas las fechas necesarias ya estaban cacheadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Insertar las fechas nuevas
# MAGIC Correr solo si la celda anterior confirmó que hay filas nuevas — si no,
# MAGIC esta celda fallaría al no existir la vista temporal.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gran_concepcion.02_plata.valores_pesos
# MAGIC     (fecha_valor, valor_uf_clp, valor_dolar_clp, fecha_consulta)
# MAGIC SELECT
# MAGIC     CAST(fecha_valor AS DATE),
# MAGIC     valor_uf_clp,
# MAGIC     valor_dolar_clp,
# MAGIC     CAST(fecha_consulta AS DATE)
# MAGIC FROM valores_pesos_nuevos_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Verificar
# MAGIC Confirma cuántas fechas hay cacheadas en total, y muestra las más recientes.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_fechas_cacheadas FROM gran_concepcion.02_plata.valores_pesos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gran_concepcion.02_plata.valores_pesos ORDER BY fecha_valor DESC LIMIT 10