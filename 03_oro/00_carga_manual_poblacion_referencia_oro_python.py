# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 00 — Carga manual de la población de referencia (Oro)
# MAGIC
# MAGIC El modelo de precio del proyecto original se entrenó sobre un dataset
# MAGIC histórico congelado, no sobre "todo lo que haya en el catálogo al momento
# MAGIC de la corrida". Para que un aviso nuevo reciba exactamente las mismas
# MAGIC features que recibiría en el proyecto original (mismos vecinos
# MAGIC geográficos, mismo nivel de barrio, mismas medianas de respaldo), Oro
# MAGIC tiene que comparar cada aviso nuevo contra esa MISMA población congelada,
# MAGIC no contra el resto de avisos que vayan llegando.
# MAGIC
# MAGIC Este notebook carga esa población congelada, junto con los artefactos que
# MAGIC la acompañan, a tablas Delta que `06_features_oro_sql` usa como
# MAGIC referencia fija. **Corrida manual, una sola vez** (o cada vez que se
# MAGIC entrene una versión nueva del modelo con un dataset de referencia
# MAGIC distinto) — no forma parte de la secuencia automática del pipeline.
# MAGIC
# MAGIC **Qué recibe** (subido a mano a un Volume antes de correr este notebook,
# MAGIC mismos archivos que ya usa el proyecto original en
# MAGIC `investigacion/03_ingenieria_variables/save/`):
# MAGIC - `datos_ingenieria_variables.csv`: el dataset histórico ya limpio e
# MAGIC   imputado (sin latitud/longitud/comuna, esas se recuperan de la base
# MAGIC   original).
# MAGIC - `niveles_barrio.json`: diccionario barrio → nivel de precio, ya
# MAGIC   calculado, más el nivel por defecto para barrios no vistos.
# MAGIC - `selected_features.csv`: la lista de las features que el modelo espera.
# MAGIC - `avisos_gran_concepcion.db`: la base SQLite ORIGINAL de investigación,
# MAGIC   de donde se recupera latitud/longitud/comuna por `id_aviso` (se abre en
# MAGIC   modo solo lectura).
# MAGIC
# MAGIC **Qué entrega:** cuatro tablas en `gran_concepcion.03_oro`:
# MAGIC - `poblacion_referencia`: una fila por aviso histórico, con coordenadas.
# MAGIC - `niveles_barrio_referencia`: mapa barrio → nivel.
# MAGIC - `referencia_estadisticas`: valores de respaldo (clave/valor) usados
# MAGIC   cuando un aviso nuevo no tiene vecinos válidos.
# MAGIC - `referencia_estadisticas_por_comuna`: los mismos respaldos, por comuna.
# MAGIC - `features_seleccionadas`: la lista de features que espera el modelo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Importar librerías

# COMMAND ----------

import json
import sqlite3

import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuración: rutas de los artefactos en el Volume
# MAGIC Ajustar si se subieron a otra carpeta.

# COMMAND ----------

RUTA_REFERENCIA = "/Volumes/gran_concepcion/03_oro/referencia_modelo"
RUTA_CSV_INGENIERIA_VARIABLES = f"{RUTA_REFERENCIA}/datos_ingenieria_variables.csv"
RUTA_NIVELES_BARRIO_JSON = f"{RUTA_REFERENCIA}/niveles_barrio.json"
RUTA_SELECTED_FEATURES_CSV = f"{RUTA_REFERENCIA}/selected_features.csv"
RUTA_BD_ORIGINAL_SQLITE = f"{RUTA_REFERENCIA}/avisos_gran_concepcion.db"

# Mismo multiplicador de IQR que usa el proyecto original para descartar
# outliers de precio/m2 al buscar comparables de sector.
MULTIPLICADOR_IQR = 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Crear el esquema y las tablas de referencia (si no existen)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gran_concepcion.03_oro")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.poblacion_referencia (
        id_aviso          STRING NOT NULL,
        comuna            STRING,
        latitud           DOUBLE,
        longitud          DOUBLE,
        antiguedad_anos   DOUBLE,
        piso_unidad       DOUBLE,
        precio_m2         DOUBLE,
        precio_m2_valido  BOOLEAN,
        rank_nac          DOUBLE,
        pob_rsh_uv        DOUBLE,
        p_urbano          DOUBLE,
        c_ig_com          DOUBLE,
        hog_uv            DOUBLE
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.niveles_barrio_referencia (
        barrio       STRING NOT NULL,
        nivel_barrio INT NOT NULL
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.referencia_estadisticas (
        clave STRING NOT NULL,
        valor DOUBLE
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.referencia_estadisticas_por_comuna (
        comuna            STRING NOT NULL,
        mediana_antiguedad DOUBLE,
        media_rank_nac    DOUBLE,
        media_pob_rsh_uv  DOUBLE,
        media_p_urbano    DOUBLE,
        media_c_ig_com    DOUBLE,
        media_hog_uv      DOUBLE
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.features_seleccionadas (
        feature STRING NOT NULL
    )
""")

print("Esquema y tablas de referencia verificados/creados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Leer el CSV histórico y recuperar comuna/latitud/longitud
# MAGIC `datos_ingenieria_variables.csv` no trae coordenadas (se descartan en el
# MAGIC pipeline de investigación tras usarlas para imputar), así que se
# MAGIC recuperan con una consulta de solo lectura contra la base SQLite
# MAGIC ORIGINAL, igual que hace `04_ingenieria_variables_produccion.py` en el
# MAGIC proyecto original.

# COMMAND ----------

df_csv = pd.read_csv(RUTA_CSV_INGENIERIA_VARIABLES)

con_original = sqlite3.connect(f"file:{RUTA_BD_ORIGINAL_SQLITE}?mode=ro", uri=True)
coords_comuna = pd.read_sql_query("""
    SELECT a.id_aviso, a.comuna, d.latitud, d.longitud
    FROM avisos a
    JOIN avisos_detalle d ON a.id_aviso = d.id_aviso
    WHERE a.tipo_propiedad = 'departamento'
""", con_original)
con_original.close()

referencia = df_csv.merge(coords_comuna, on="id_aviso", how="inner")
referencia["latitud"] = pd.to_numeric(referencia["latitud"], errors="coerce")
referencia["longitud"] = pd.to_numeric(referencia["longitud"], errors="coerce")
referencia = referencia.dropna(subset=["latitud", "longitud"]).reset_index(drop=True)

print(f"Población de referencia: {len(referencia)} departamentos históricos con coordenadas "
      f"(de {len(df_csv)} en el CSV original).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Calcular `precio_m2` y marcar los valores razonables (IQR global)
# MAGIC Basado en `costo_total_clp` (arriendo + gastos comunes), no en el
# MAGIC arriendo nominal, para que quede en la misma escala que el target del
# MAGIC modelo. El límite IQR se calcula UNA vez sobre toda la población (no por
# MAGIC aviso, no por vecindario): así los vecinos que devuelva
# MAGIC `06_features_oro_sql` para un aviso nuevo son consistentes entre sí, igual
# MAGIC que en el proyecto original.

# COMMAND ----------

referencia["precio_m2"] = referencia["costo_total_clp"] / referencia["superficie_util_m2"].replace(0, np.nan)

precio_m2_notna = referencia["precio_m2"].dropna()
q1, q3 = precio_m2_notna.quantile([0.25, 0.75])
iqr = q3 - q1
lim_inf_precio_m2 = q1 - MULTIPLICADOR_IQR * iqr
lim_sup_precio_m2 = q3 + MULTIPLICADOR_IQR * iqr

referencia["precio_m2_valido"] = referencia["precio_m2"].notna() & referencia["precio_m2"].between(
    lim_inf_precio_m2, lim_sup_precio_m2
)

mediana_precio_m2_fallback = float(referencia.loc[referencia["precio_m2_valido"], "precio_m2"].median())

print(f"Límite IQR de precio/m2: [{lim_inf_precio_m2:,.0f}, {lim_sup_precio_m2:,.0f}] | "
      f"Mediana de respaldo: {mediana_precio_m2_fallback:,.0f} | "
      f"{referencia['precio_m2_valido'].sum()} de {len(referencia)} avisos con precio/m2 válido.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Guardar `poblacion_referencia`
# MAGIC Reemplaza la tabla completa (no incremental): esta carga es manual y
# MAGIC representa una única foto congelada del dataset de entrenamiento vigente.

# COMMAND ----------

columnas_poblacion = [
    "id_aviso", "comuna", "latitud", "longitud", "antiguedad_anos", "piso_unidad",
    "precio_m2", "precio_m2_valido", "rank_nac", "pob_rsh_uv", "p_urbano", "c_ig_com", "hog_uv",
]

spark.createDataFrame(referencia[columnas_poblacion]).write.mode("overwrite").saveAsTable(
    "gran_concepcion.03_oro.poblacion_referencia"
)

print(f"{referencia.shape[0]} filas guardadas en poblacion_referencia.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Calcular y guardar las estadísticas de respaldo (global y por comuna)
# MAGIC Todo SQL a partir de acá: la tabla ya está en Delta, así que las medianas
# MAGIC y medias se calculan directo sobre `poblacion_referencia`, sin volver a
# MAGIC tocar pandas.

# COMMAND ----------

spark.sql("""
    INSERT OVERWRITE TABLE gran_concepcion.03_oro.referencia_estadisticas_por_comuna
    SELECT
        comuna,
        PERCENTILE_APPROX(antiguedad_anos, 0.5) AS mediana_antiguedad,
        AVG(rank_nac)     AS media_rank_nac,
        AVG(pob_rsh_uv)   AS media_pob_rsh_uv,
        AVG(p_urbano)     AS media_p_urbano,
        AVG(c_ig_com)     AS media_c_ig_com,
        AVG(hog_uv)       AS media_hog_uv
    FROM gran_concepcion.03_oro.poblacion_referencia
    GROUP BY comuna
""")

spark.sql(f"""
    INSERT OVERWRITE TABLE gran_concepcion.03_oro.referencia_estadisticas
    SELECT 'mediana_antiguedad_global' AS clave, PERCENTILE_APPROX(antiguedad_anos, 0.5) AS valor
    FROM gran_concepcion.03_oro.poblacion_referencia
    UNION ALL
    SELECT 'piso_promedio', AVG(piso_unidad) FROM gran_concepcion.03_oro.poblacion_referencia
    UNION ALL
    SELECT 'media_rank_nac_global', AVG(rank_nac) FROM gran_concepcion.03_oro.poblacion_referencia
    UNION ALL
    SELECT 'media_pob_rsh_uv_global', AVG(pob_rsh_uv) FROM gran_concepcion.03_oro.poblacion_referencia
    UNION ALL
    SELECT 'media_p_urbano_global', AVG(p_urbano) FROM gran_concepcion.03_oro.poblacion_referencia
    UNION ALL
    SELECT 'media_c_ig_com_global', AVG(c_ig_com) FROM gran_concepcion.03_oro.poblacion_referencia
    UNION ALL
    SELECT 'media_hog_uv_global', AVG(hog_uv) FROM gran_concepcion.03_oro.poblacion_referencia
    UNION ALL
    SELECT 'lim_inf_precio_m2', {lim_inf_precio_m2}
    UNION ALL
    SELECT 'lim_sup_precio_m2', {lim_sup_precio_m2}
    UNION ALL
    SELECT 'mediana_precio_m2_fallback', {mediana_precio_m2_fallback}
""")

print("Estadísticas de respaldo (global y por comuna) calculadas y guardadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Cargar `niveles_barrio.json`
# MAGIC El nivel por defecto (para barrios que no estaban en el diccionario al
# MAGIC momento de entrenar) se guarda como una fila más en
# MAGIC `referencia_estadisticas`, con clave `nivel_barrio_default`.

# COMMAND ----------

with open(RUTA_NIVELES_BARRIO_JSON, encoding="utf-8") as f:
    niveles_barrio = json.load(f)

df_niveles_barrio = pd.DataFrame([
    {"barrio": barrio, "nivel_barrio": int(nivel)}
    for barrio, nivel in niveles_barrio["mapa_barrio_a_nivel"].items()
])

spark.createDataFrame(df_niveles_barrio).write.mode("overwrite").saveAsTable(
    "gran_concepcion.03_oro.niveles_barrio_referencia"
)

spark.sql(f"""
    DELETE FROM gran_concepcion.03_oro.referencia_estadisticas WHERE clave = 'nivel_barrio_default'
""")
spark.sql(f"""
    INSERT INTO gran_concepcion.03_oro.referencia_estadisticas
    VALUES ('nivel_barrio_default', {float(niveles_barrio["nivel_default"])})
""")

print(f"{df_niveles_barrio.shape[0]} barrios cargados. Nivel por defecto: {niveles_barrio['nivel_default']}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Cargar `selected_features.csv`

# COMMAND ----------

df_features = pd.read_csv(RUTA_SELECTED_FEATURES_CSV)

spark.createDataFrame(df_features[["feature"]]).write.mode("overwrite").saveAsTable(
    "gran_concepcion.03_oro.features_seleccionadas"
)

print(f"{df_features.shape[0]} features seleccionadas cargadas: {df_features['feature'].tolist()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.poblacion_referencia) AS avisos_referencia,
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.niveles_barrio_referencia) AS barrios,
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.referencia_estadisticas) AS estadisticas_globales,
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.referencia_estadisticas_por_comuna) AS comunas,
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.features_seleccionadas) AS features
