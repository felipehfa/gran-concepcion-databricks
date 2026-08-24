# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "scikit-learn==1.9.0",
# ]
# ///
# MAGIC %md
# MAGIC # 05 — Imputación de superficie con Random Forest (Plata)
# MAGIC
# MAGIC Carga dos modelos `RandomForestRegressor` ya entrenados (uno para
# MAGIC `superficie_util_m2`, otro para `superficie_total_m2`) y los usa para
# MAGIC completar los avisos donde ese dato faltaba tras la limpieza SQL. Solo se
# MAGIC predicen las filas con valor faltante — las que ya tienen un valor válido
# MAGIC (real o imputado en una corrida anterior) no se tocan.
# MAGIC
# MAGIC Se corrige primero **superficie útil**, se guarda ese resultado, y recién
# MAGIC después se corrige **superficie total** — ya que el modelo de superficie
# MAGIC total usa la superficie útil (ya corregida) como una de sus columnas
# MAGIC predictoras.
# MAGIC
# MAGIC Cada valor imputado queda marcado con `superficie_util_imputada` /
# MAGIC `superficie_total_imputada` (booleano) y con `fecha_imputacion_util` /
# MAGIC `fecha_imputacion_total` (momento exacto en que se calculó).
# MAGIC
# MAGIC **Requisito previo:**
# MAGIC - El notebook `04_limpieza_plata_sql` ya debe haber corrido, generando o
# MAGIC   actualizando `gran_concepcion.02_plata.avisos_limpios`.
# MAGIC - Ambos `.pkl` subidos a `/Volumes/gran_concepcion/02_plata/modelos/`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Importar librerías
# MAGIC `scikit-learn` ya viene declarada como dependencia del entorno de este
# MAGIC notebook (bloque `dependencies` arriba) — necesaria para que `joblib`
# MAGIC pueda deserializar los modelos `RandomForestRegressor` guardados, aunque
# MAGIC no se importe explícitamente acá.

# COMMAND ----------

# MAGIC %pip install --force-reinstall "numpy<2" "scipy<1.14" scikit-learn==1.9.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime

import joblib
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Asegurar que existan las columnas de trazabilidad
# MAGIC Si `avisos_limpios` viene de una corrida anterior de este mismo notebook,
# MAGIC estas columnas ya existen y el `ALTER TABLE` falla silenciosamente (se
# MAGIC captura la excepción). Si es la primera vez, se crean.

# COMMAND ----------

columnas_a_asegurar = {
    "superficie_util_imputada": "BOOLEAN",
    "superficie_total_imputada": "BOOLEAN",
    "fecha_imputacion_util": "TIMESTAMP",
    "fecha_imputacion_total": "TIMESTAMP",
}

for columna, tipo in columnas_a_asegurar.items():
    try:
        spark.sql(f"""
            ALTER TABLE gran_concepcion.02_plata.avisos_limpios
            ADD COLUMNS ({columna} {tipo})
        """)
        print(f"Columna '{columna}' agregada.")
    except Exception as e:
        print(f"Columna '{columna}' ya existía (o error menor): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Cargar el modelo de superficie útil

# COMMAND ----------

RUTA_MODELO_METROS_UTILES = "/Volumes/gran_concepcion/02_plata/modelos/modelo_superficie_util_m2_departamento.pkl"
paquete_util = joblib.load(RUTA_MODELO_METROS_UTILES)

print(f"Columna objetivo: {paquete_util['columna_objetivo']}")
print(f"Grupo: {paquete_util.get('grupo')}")
print(f"Columnas de entrenamiento: {paquete_util['columnas_entrenamiento']}")
print(f"MAE validación: {paquete_util.get('mae_validacion')}")
print(f"R² validación: {paquete_util.get('r2_validacion')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Leer la tabla de Plata

# COMMAND ----------

df_plata = spark.sql("SELECT * FROM gran_concepcion.02_plata.avisos_limpios").toPandas()
print(f"{len(df_plata)} filas en total")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Identificar avisos con superficie útil faltante

# COMMAND ----------

mask_falta_util = df_plata["superficie_util_m2"].isna()
print(f"{mask_falta_util.sum()} avisos con superficie_util_m2 faltante de {len(df_plata)} totales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Predecir y completar superficie útil
# MAGIC Se generan columnas dummy de comuna para armar el input exacto que el
# MAGIC modelo espera, sin tocar `df_plata`. Si no hay nada pendiente, no se llama
# MAGIC al modelo (evita el error de "0 samples").

# COMMAND ----------

ahora = datetime.now()

if mask_falta_util.sum() == 0:
    print("No hay valores faltantes de superficie_util_m2 — nada que imputar.")
else:
    columnas_util = paquete_util["columnas_entrenamiento"]

    comuna_dummies = pd.get_dummies(df_plata["comuna"], prefix="comuna")
    df_features = pd.concat([df_plata, comuna_dummies], axis=1)

    X_falta_util = df_features.loc[mask_falta_util].reindex(columns=columnas_util, fill_value=0)
    predicciones_util = paquete_util["modelo"].predict(X_falta_util)

    df_plata.loc[mask_falta_util, "superficie_util_m2"] = predicciones_util
    df_plata.loc[mask_falta_util, "superficie_util_imputada"] = True
    df_plata.loc[mask_falta_util, "fecha_imputacion_util"] = ahora

    print(f"{mask_falta_util.sum()} valores de superficie_util_m2 imputados")

df_plata.loc[mask_falta_util, ["id_aviso", "superficie_util_m2", "superficie_util_imputada", "fecha_imputacion_util"]].head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Guardar superficie útil corregida en la tabla de Plata
# MAGIC `MERGE` dirigido solo a las filas imputadas esta corrida y solo a las 3
# MAGIC columnas relevantes (`superficie_util_m2`, `superficie_util_imputada`,
# MAGIC `fecha_imputacion_util`) — **no** un `CREATE OR REPLACE TABLE ... AS
# MAGIC SELECT *` sobre todo `avisos_limpios`. Ese patrón (usado antes acá)
# MAGIC reescribía la tabla completa pasando cada corrida por un round-trip a
# MAGIC pandas (`spark.createDataFrame(df_plata)`), que reinfiere el tipo de
# MAGIC TODAS las columnas desde los dtypes de pandas — no solo las que este
# MAGIC notebook realmente toca. Eso pisaba en silencio, en cada corrida,
# MAGIC cualquier tipo explícito fijado aguas arriba (ej. los 7 flags booleanos
# MAGIC de `04_limpieza_plata_sql.py`, que pandas infiere `float64` en cuanto hay
# MAGIC un solo NULL). El `MERGE` no puede tocar el esquema de columnas que ni
# MAGIC siquiera referencia.

# COMMAND ----------

if mask_falta_util.sum() > 0:
    columnas_actualizar_util = [
        "id_aviso", "superficie_util_m2", "superficie_util_imputada", "fecha_imputacion_util",
    ]
    spark.createDataFrame(
        df_plata.loc[mask_falta_util, columnas_actualizar_util]
    ).createOrReplaceTempView("util_imputado_tmp")

    spark.sql("""
        MERGE INTO gran_concepcion.02_plata.avisos_limpios AS t
        USING util_imputado_tmp AS s
        ON t.id_aviso = s.id_aviso
        WHEN MATCHED THEN UPDATE SET
            t.superficie_util_m2 = s.superficie_util_m2,
            t.superficie_util_imputada = s.superficie_util_imputada,
            t.fecha_imputacion_util = s.fecha_imputacion_util
    """)
    print(f"{mask_falta_util.sum()} filas actualizadas en avisos_limpios (superficie útil).")
else:
    print("Nada que guardar: no hubo valores de superficie_util_m2 imputados esta corrida.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Verificar superficie útil antes de continuar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_filas,
# MAGIC     SUM(CASE WHEN superficie_util_imputada THEN 1 ELSE 0 END) AS util_imputados,
# MAGIC     SUM(CASE WHEN superficie_util_m2 IS NULL THEN 1 ELSE 0 END) AS nulos_remanentes_util
# MAGIC FROM gran_concepcion.02_plata.avisos_limpios

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Cargar el modelo de superficie total
# MAGIC Se vuelve a leer la tabla de Plata (ya con superficie útil corregida y
# MAGIC guardada) para usarla como input de este segundo modelo.

# COMMAND ----------

RUTA_MODELO_METROS_TOTALES = "/Volumes/gran_concepcion/02_plata/modelos/modelo_superficie_total_m2_departamento.pkl"
paquete_total = joblib.load(RUTA_MODELO_METROS_TOTALES)

print(f"Columna objetivo: {paquete_total['columna_objetivo']}")
print(f"Grupo: {paquete_total.get('grupo')}")
print(f"Columnas de entrenamiento: {paquete_total['columnas_entrenamiento']}")
print(f"MAE validación: {paquete_total.get('mae_validacion')}")
print(f"R² validación: {paquete_total.get('r2_validacion')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Releer la tabla de Plata, ya con superficie útil corregida
# MAGIC Importante: se relee desde la tabla (no se reutiliza el `df_plata` en
# MAGIC memoria de la sección anterior), para partir de los datos ya guardados.

# COMMAND ----------

df_plata = spark.sql("SELECT * FROM gran_concepcion.02_plata.avisos_limpios").toPandas()
print(f"{len(df_plata)} filas en total")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Identificar avisos con superficie total faltante

# COMMAND ----------

mask_falta_total = df_plata["superficie_total_m2"].isna()
print(f"{mask_falta_total.sum()} avisos con superficie_total_m2 faltante de {len(df_plata)} totales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Predecir y completar superficie total

# COMMAND ----------

ahora = datetime.now()

if mask_falta_total.sum() == 0:
    print("No hay valores faltantes de superficie_total_m2 — nada que imputar.")
else:
    columnas_total = paquete_total["columnas_entrenamiento"]

    comuna_dummies = pd.get_dummies(df_plata["comuna"], prefix="comuna")
    df_features = pd.concat([df_plata, comuna_dummies], axis=1)

    X_falta_total = df_features.loc[mask_falta_total].reindex(columns=columnas_total, fill_value=0)
    predicciones_total = paquete_total["modelo"].predict(X_falta_total)

    df_plata.loc[mask_falta_total, "superficie_total_m2"] = predicciones_total
    df_plata.loc[mask_falta_total, "superficie_total_imputada"] = True
    df_plata.loc[mask_falta_total, "fecha_imputacion_total"] = ahora

    print(f"{mask_falta_total.sum()} valores de superficie_total_m2 imputados")

df_plata.loc[mask_falta_total, ["id_aviso", "superficie_total_m2", "superficie_total_imputada", "fecha_imputacion_total"]].head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Guardar superficie total corregida en la tabla de Plata
# MAGIC Mismo `MERGE` dirigido que en la sección 6 — ver esa celda para el porqué
# MAGIC del cambio respecto al `CREATE OR REPLACE TABLE ... AS SELECT *` que
# MAGIC usaba antes.

# COMMAND ----------

if mask_falta_total.sum() > 0:
    columnas_actualizar_total = [
        "id_aviso", "superficie_total_m2", "superficie_total_imputada", "fecha_imputacion_total",
    ]
    spark.createDataFrame(
        df_plata.loc[mask_falta_total, columnas_actualizar_total]
    ).createOrReplaceTempView("total_imputado_tmp")

    spark.sql("""
        MERGE INTO gran_concepcion.02_plata.avisos_limpios AS t
        USING total_imputado_tmp AS s
        ON t.id_aviso = s.id_aviso
        WHEN MATCHED THEN UPDATE SET
            t.superficie_total_m2 = s.superficie_total_m2,
            t.superficie_total_imputada = s.superficie_total_imputada,
            t.fecha_imputacion_total = s.fecha_imputacion_total
    """)
    print(f"{mask_falta_total.sum()} filas actualizadas en avisos_limpios (superficie total).")
else:
    print("Nada que guardar: no hubo valores de superficie_total_m2 imputados esta corrida.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. Verificación final

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_filas,
# MAGIC     SUM(CASE WHEN superficie_util_imputada THEN 1 ELSE 0 END) AS util_imputados,
# MAGIC     SUM(CASE WHEN superficie_total_imputada THEN 1 ELSE 0 END) AS total_imputados,
# MAGIC     SUM(CASE WHEN superficie_util_m2 IS NULL THEN 1 ELSE 0 END) AS nulos_remanentes_util,
# MAGIC     SUM(CASE WHEN superficie_total_m2 IS NULL THEN 1 ELSE 0 END) AS nulos_remanentes_total
# MAGIC FROM gran_concepcion.02_plata.avisos_limpios
