# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "pyshp==2.3.1",
#   "shapely==2.1.2",
# ]
# ///
# MAGIC %md
# MAGIC # 00 — Carga manual de polígonos de vulnerabilidad (Bronce)
# MAGIC
# MAGIC Lee el shapefile IGVUST (Índice de Vulnerabilidad Socioterritorial, por
# MAGIC Unidad Vecinal) desde un Volume, lo recorta a las 10 comunas del Gran
# MAGIC Concepción que usa el resto del pipeline, reproyecta cada polígono de Web
# MAGIC Mercator (EPSG:3857, la proyección en la que viene el shapefile original)
# MAGIC a WGS84 (EPSG:4326, lat/lon) y guarda el resultado como WKT en
# MAGIC `gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv`.
# MAGIC
# MAGIC **Corrida manual, una sola vez** (o cada vez que el shapefile IGVUST se
# MAGIC actualice): esta tabla no cambia con cada corrida del scraper, así que no
# MAGIC forma parte de la secuencia automática de notebooks. El resto del pipeline
# MAGIC (`07_vulnerabilidad_oro_python`) solo lee de esta tabla ya poblada, nunca
# MAGIC del shapefile.
# MAGIC
# MAGIC **Qué recibe:** el shapefile IGVUST (`.shp`/`.shx`/`.dbf`/`.prj`) ya subido
# MAGIC a un Volume de Databricks (ruta configurable más abajo).
# MAGIC
# MAGIC **Qué entrega:** la tabla `poligonos_vulnerabilidad_uv` poblada con una
# MAGIC fila por Unidad Vecinal de las 10 comunas analizadas, lista para que
# MAGIC `07_vulnerabilidad_oro_python` resuelva el cruce punto-en-polígono de cada
# MAGIC aviso.
# MAGIC
# MAGIC **Por qué sin geopandas:** el shapefile viene en Web Mercator, no en
# MAGIC WGS84. La conversión entre ambos sistemas tiene una fórmula cerrada simple
# MAGIC (no es un cambio de datum genérico), así que alcanza con `pyshp` (lectura
# MAGIC pura de shapefile, sin GDAL) + `shapely` (geometría y WKT) + la fórmula de
# MAGIC reproyección escrita a mano más abajo. Esto evita depender de
# MAGIC geopandas/GDAL/fiona, que son mucho más pesados de instalar en un cluster
# MAGIC de Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Importar librerías
# MAGIC `pyshp` y `shapely` ya vienen declaradas como dependencias del entorno de
# MAGIC este notebook (bloque `dependencies` arriba), así que no hace falta un
# MAGIC `%pip install` adicional; solo hay que importarlas.

# COMMAND ----------

import math

import pandas as pd
import shapefile
from shapely.geometry import shape as shapely_shape
from shapely.ops import transform as shapely_transform

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Crear el esquema de Bronce y la tabla `poligonos_vulnerabilidad_uv` (si no existen)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gran_concepcion.01_bronce")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv (
        uv_rsh         STRING NOT NULL,
        comuna         STRING NOT NULL,
        rank_nac       DOUBLE,
        pob_rsh_uv     DOUBLE,
        p_urbano       DOUBLE,
        c_ig_com       DOUBLE,
        hog_uv         DOUBLE,
        geometria_wkt  STRING NOT NULL
    )
""")

print("Tabla poligonos_vulnerabilidad_uv verificada/creada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Configuración: ruta del shapefile y comunas del Gran Concepción
# MAGIC Ajustar `RUTA_SHAPEFILE` a donde se haya subido el `.shp` en el Volume
# MAGIC (los archivos `.shx`, `.dbf` y `.prj` deben estar en la misma carpeta, con
# MAGIC el mismo nombre base). `COMUNAS_ANALIZADAS` mapea el slug de comuna (como
# MAGIC en la columna `comuna` del resto del pipeline) al nombre tal como aparece
# MAGIC en el shapefile (columna `Comuna`, mayúsculas sin tildes) — mismo mapeo que
# MAGIC usa el proyecto original.

# COMMAND ----------

RUTA_SHAPEFILE = "/Volumes/gran_concepcion/01_bronce/vulnerabilidad/202505_IGVUST_UV_cuartil.shp"

COMUNAS_ANALIZADAS = {
    "concepcion-biobio": "CONCEPCION",
    "talcahuano-biobio": "TALCAHUANO",
    "hualpen-biobio": "HUALPEN",
    "san-pedro-de-la-paz-biobio": "SAN PEDRO DE LA PAZ",
    "chiguayante-biobio": "CHIGUAYANTE",
    "penco-biobio": "PENCO",
    "tome-biobio": "TOME",
    "coronel-biobio": "CORONEL",
    "hualqui-biobio": "HUALQUI",
    "lota-biobio": "LOTA",
}

# Web Mercator (EPSG:3857): mismo radio de esfera auxiliar que usa el
# shapefile IGVUST (ver su .prj: "WGS_1984_Web_Mercator_Auxiliary_Sphere").
RADIO_WEB_MERCATOR_M = 6378137.0

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Reproyección Web Mercator (EPSG:3857) -> WGS84 (EPSG:4326)
# MAGIC Fórmula cerrada estándar de la Mercator esférica — exacta para esta
# MAGIC proyección, no es una aproximación.

# COMMAND ----------

def web_mercator_a_wgs84(x, y):
    lon = (x / RADIO_WEB_MERCATOR_M) * (180.0 / math.pi)
    lat = (2 * math.atan(math.exp(y / RADIO_WEB_MERCATOR_M)) - math.pi / 2) * (180.0 / math.pi)
    return lon, lat

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Leer el shapefile y quedarse solo con las 10 comunas del Gran Concepción
# MAGIC Se usa `shape.__geo_interface__` (que ya trae los anillos de cada polígono
# MAGIC correctamente agrupados por `pyshp`, exterior + huecos si los hay) en vez
# MAGIC de reconstruirlos a mano, y se reproyecta cada vértice con
# MAGIC `shapely.ops.transform`.

# COMMAND ----------

nombres_comuna_validos = set(COMUNAS_ANALIZADAS.values())

lector = shapefile.Reader(RUTA_SHAPEFILE)
registros_filtrados = []

for registro in lector.shapeRecords():
    atributos = registro.record.as_dict()
    nombre_comuna = str(atributos.get("Comuna", "")).strip().upper()

    if nombre_comuna not in nombres_comuna_validos:
        continue

    geometria_wm = shapely_shape(registro.shape.__geo_interface__)
    geometria_wgs84 = shapely_transform(web_mercator_a_wgs84, geometria_wm)

    registros_filtrados.append({
        "uv_rsh": str(atributos.get("uv_rsh")),
        "comuna": nombre_comuna,
        "rank_nac": atributos.get("rank_nac"),
        "pob_rsh_uv": atributos.get("pob_rsh_uv"),
        "p_urbano": atributos.get("p_urbano"),
        "c_ig_com": atributos.get("c_ig_com"),
        "hog_uv": atributos.get("hog_uv"),
        "geometria_wkt": geometria_wgs84.wkt,
    })

print(f"{len(registros_filtrados)} Unidades Vecinales encontradas en las 10 comunas del Gran Concepción "
      f"(de {len(lector)} en el shapefile completo).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Armar DataFrame y vista temporal

# COMMAND ----------

df_poligonos = pd.DataFrame(registros_filtrados)
spark.createDataFrame(df_poligonos).createOrReplaceTempView("poligonos_vulnerabilidad_tmp")
df_poligonos.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. MERGE hacia Bronce (upsert por `uv_rsh`)
# MAGIC Idempotente: si se vuelve a correr con el mismo shapefile (o uno
# MAGIC actualizado), actualiza las Unidades Vecinales ya existentes en vez de
# MAGIC duplicarlas.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv AS destino
# MAGIC USING poligonos_vulnerabilidad_tmp AS nuevo
# MAGIC ON destino.uv_rsh = nuevo.uv_rsh
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     destino.comuna = nuevo.comuna,
# MAGIC     destino.rank_nac = nuevo.rank_nac,
# MAGIC     destino.pob_rsh_uv = nuevo.pob_rsh_uv,
# MAGIC     destino.p_urbano = nuevo.p_urbano,
# MAGIC     destino.c_ig_com = nuevo.c_ig_com,
# MAGIC     destino.hog_uv = nuevo.hog_uv,
# MAGIC     destino.geometria_wkt = nuevo.geometria_wkt
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     uv_rsh, comuna, rank_nac, pob_rsh_uv, p_urbano, c_ig_com, hog_uv, geometria_wkt
# MAGIC ) VALUES (
# MAGIC     nuevo.uv_rsh, nuevo.comuna, nuevo.rank_nac, nuevo.pob_rsh_uv,
# MAGIC     nuevo.p_urbano, nuevo.c_ig_com, nuevo.hog_uv, nuevo.geometria_wkt
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT comuna, COUNT(*) AS unidades_vecinales
# MAGIC FROM gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv
# MAGIC GROUP BY comuna
# MAGIC ORDER BY comuna
