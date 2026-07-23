# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "shapely",
# ]
# ///
# MAGIC %md
# MAGIC # 07 — Vulnerabilidad socioterritorial (Oro)
# MAGIC
# MAGIC Resuelve `uv_rsh`, `rank_nac`, `pob_rsh_uv`, `p_urbano`, `c_ig_com` y
# MAGIC `hog_uv` para cada aviso, cruzando su coordenada contra los polígonos de
# MAGIC Unidad Vecinal (IGVUST) mediante un test punto-en-polígono con `shapely`.
# MAGIC
# MAGIC Corre DESPUÉS de `06_features_oro_sql`, que ya dejó estas mismas columnas
# MAGIC (salvo `uv_rsh`) con un valor de respaldo (media de la comuna en la
# MAGIC población de referencia). El `MERGE` de acá abajo sobreescribe ese
# MAGIC respaldo con el valor real en cuanto el cruce punto-en-polígono lo
# MAGIC resuelve — mismo orden de prioridad que usa el proyecto original (valor
# MAGIC real > media de la comuna > media global).
# MAGIC
# MAGIC Se hace directamente en Oro (no en Plata): es un enriquecimiento por
# MAGIC coordenadas geográficas, igual en naturaleza a las distancias Haversine
# MAGIC que ya se calculan acá — no es una corrección de datos sucios, es una
# MAGIC feature más derivada de `latitud`/`longitud`.
# MAGIC
# MAGIC **Incremental:** solo procesa avisos con coordenadas válidas y `uv_rsh`
# MAGIC todavía NULL. Una vez resuelto, no se vuelve a tocar (el cruce no cambia
# MAGIC salvo que el shapefile de origen se actualice y se re-suba a Bronce).
# MAGIC
# MAGIC **Requisito previo:**
# MAGIC - `gran_concepcion.03_oro.avisos_features` ya generada (notebook 06).
# MAGIC - `gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv` con datos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Instalar shapely (si hace falta)

# COMMAND ----------

# MAGIC %pip install shapely

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Importar librerías

# COMMAND ----------

from datetime import datetime

import pandas as pd
from shapely import wkt
from shapely.geometry import Point

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Asegurar que existan las columnas de vulnerabilidad en Oro
# MAGIC Si ya existen (de una corrida anterior), el `ALTER TABLE` falla
# MAGIC silenciosamente (se captura la excepción).

# COMMAND ----------

columnas_a_asegurar = {
    "uv_rsh": "STRING",
    "rank_nac": "DOUBLE",
    "pob_rsh_uv": "DOUBLE",
    "p_urbano": "DOUBLE",
    "c_ig_com": "DOUBLE",
    "hog_uv": "DOUBLE",
    "fecha_vulnerabilidad_oro": "TIMESTAMP",
}

for columna, tipo in columnas_a_asegurar.items():
    try:
        spark.sql(f"""
            ALTER TABLE gran_concepcion.03_oro.avisos_features
            ADD COLUMNS ({columna} {tipo})
        """)
        print(f"Columna '{columna}' agregada.")
    except Exception as e:
        print(f"Columna '{columna}' ya existía (o error menor): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Cargar los polígonos de Unidad Vecinal desde Bronce
# MAGIC Parsea `geometria_wkt` (texto) a geometría real de shapely.

# COMMAND ----------

poligonos_rows = spark.sql("""
    SELECT uv_rsh, comuna, rank_nac, pob_rsh_uv, p_urbano, c_ig_com, hog_uv, geometria_wkt
    FROM gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv
""").collect()

if not poligonos_rows:
    raise RuntimeError(
        "gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv está vacía. "
        "Subí los polígonos IGVUST a Bronce antes de correr este notebook."
    )

poligonos = [
    {
        "uv_rsh": r["uv_rsh"], "comuna": r["comuna"], "rank_nac": r["rank_nac"],
        "pob_rsh_uv": r["pob_rsh_uv"], "p_urbano": r["p_urbano"], "c_ig_com": r["c_ig_com"],
        "hog_uv": r["hog_uv"], "geometria": wkt.loads(r["geometria_wkt"]),
    }
    for r in poligonos_rows
]

print(f"{len(poligonos)} polígonos de Unidad Vecinal cargados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Identificar avisos pendientes de resolver
# MAGIC Avisos en Oro con coordenadas válidas y `uv_rsh` todavía sin resolver.

# COMMAND ----------

pendientes_rows = spark.sql("""
    SELECT id_aviso, latitud, longitud
    FROM gran_concepcion.03_oro.avisos_features
    WHERE latitud IS NOT NULL AND longitud IS NOT NULL AND uv_rsh IS NULL
""").collect()

pendientes = [row.asDict() for row in pendientes_rows]
print(f"{len(pendientes)} avisos con coordenadas pendientes de resolver vulnerabilidad.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Cruce punto-en-polígono
# MAGIC Para cada aviso pendiente, se busca el primer polígono cuya geometría lo
# MAGIC contenga. Los que no caen dentro de ningún polígono (fuera de las 10
# MAGIC comunas analizadas, o coordenada imprecisa) quedan sin resolver — se
# MAGIC reintentan en la próxima corrida si el shapefile se actualiza.

# COMMAND ----------

resueltos = []
sin_uv = 0
ahora = datetime.now().isoformat()

for fila in pendientes:
    punto = Point(fila["longitud"], fila["latitud"])
    encontrado = next((p for p in poligonos if p["geometria"].contains(punto)), None)

    if encontrado is None:
        sin_uv += 1
        continue

    resueltos.append({
        "id_aviso": fila["id_aviso"],
        "uv_rsh": encontrado["uv_rsh"],
        "rank_nac": encontrado["rank_nac"],
        "pob_rsh_uv": encontrado["pob_rsh_uv"],
        "p_urbano": encontrado["p_urbano"],
        "c_ig_com": encontrado["c_ig_com"],
        "hog_uv": encontrado["hog_uv"],
        "fecha_vulnerabilidad_oro": ahora,
    })

print(f"{len(resueltos)} avisos resueltos. {sin_uv} sin Unidad Vecinal asignada "
      f"(coordenada fuera de las comunas analizadas o sin polígono coincidente).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Escribir los resultados de vuelta (MERGE, no INSERT)
# MAGIC Las filas ya existen en `avisos_features` — se actualizan solo las
# MAGIC columnas de vulnerabilidad de los avisos resueltos en esta corrida.

# COMMAND ----------

if len(resueltos) == 0:
    print("No hay avisos nuevos para actualizar.")
else:
    df_resueltos = pd.DataFrame(resueltos)
    spark.createDataFrame(df_resueltos).createOrReplaceTempView("vulnerabilidad_resuelta_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_aviso, COUNT(*) AS veces
# MAGIC FROM gran_concepcion.03_oro.avisos_features
# MAGIC GROUP BY id_aviso
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gran_concepcion.03_oro.avisos_features AS oro
# MAGIC USING vulnerabilidad_resuelta_tmp AS nuevo
# MAGIC ON oro.id_aviso = nuevo.id_aviso
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     oro.uv_rsh = nuevo.uv_rsh,
# MAGIC     oro.rank_nac = nuevo.rank_nac,
# MAGIC     oro.pob_rsh_uv = nuevo.pob_rsh_uv,
# MAGIC     oro.p_urbano = nuevo.p_urbano,
# MAGIC     oro.c_ig_com = nuevo.c_ig_com,
# MAGIC     oro.hog_uv = nuevo.hog_uv,
# MAGIC     oro.fecha_vulnerabilidad_oro = CAST(nuevo.fecha_vulnerabilidad_oro AS TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_filas,
# MAGIC     SUM(CASE WHEN uv_rsh IS NOT NULL THEN 1 ELSE 0 END) AS con_vulnerabilidad_resuelta,
# MAGIC     SUM(CASE WHEN uv_rsh IS NULL AND latitud IS NOT NULL AND longitud IS NOT NULL THEN 1 ELSE 0 END) AS pendientes_o_sin_poligono,
# MAGIC     SUM(CASE WHEN latitud IS NULL OR longitud IS NULL THEN 1 ELSE 0 END) AS sin_coordenadas
# MAGIC FROM gran_concepcion.03_oro.avisos_features