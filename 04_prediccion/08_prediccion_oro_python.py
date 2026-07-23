# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "lightgbm==4.6.0",
# ]
# ///
# MAGIC %md
# MAGIC # 08 — Predicción de precio (Predicción)
# MAGIC
# MAGIC Toma los avisos de `gran_concepcion.03_oro.avisos_features`, predice el
# MAGIC costo total mensual (arriendo + gastos comunes) con el ensamble LightGBM
# MAGIC del proyecto original, y guarda el resultado en
# MAGIC `gran_concepcion.04_prediccion.predicciones`, junto con la etiqueta
# MAGIC (oportunidad / precio de mercado / caro) y el nivel de confianza que
# MAGIC calcula la calibración guardada del modelo.
# MAGIC
# MAGIC **El modelo se carga manualmente**, no se reentrena acá: subir a un
# MAGIC Volume el `modelo_produccion.pkl` (ensamble de modelos LightGBM,
# MAGIC generado por `entrenamiento/01_entrenar_modelo_produccion.py` del
# MAGIC proyecto original) y su `parametros_produccion.json` (lista de features,
# MAGIC calibración de deciles/confianza) correspondientes a la versión vigente
# MAGIC del modelo. Si el modelo se reentrena con una feature nueva, hay que
# MAGIC subir los artefactos nuevos y, si hace falta, ajustar el código de este
# MAGIC notebook a mano.
# MAGIC
# MAGIC **Qué recibe:** `avisos_features` (Oro) + el `.pkl`/`.json` del modelo
# MAGIC vigente en el Volume.
# MAGIC
# MAGIC **Qué entrega:** una fila por `(id_aviso, version_modelo)` en
# MAGIC `predicciones`, con el costo total predicho, el z-score robusto del error,
# MAGIC el decil de precio, la etiqueta y el nivel de confianza.
# MAGIC
# MAGIC **Requisito previo:** `gran_concepcion.03_oro.avisos_features` ya
# MAGIC generada (notebooks 06 y 07 ya corridos), y el modelo vigente subido al
# MAGIC Volume configurado más abajo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuración: ruta del modelo vigente en el Volume

# COMMAND ----------

RUTA_MODELO_VIGENTE = "/Volumes/gran_concepcion/04_prediccion/modelo"
RUTA_MODELO_PKL = f"{RUTA_MODELO_VIGENTE}/modelo_produccion.pkl"
RUTA_PARAMETROS_JSON = f"{RUTA_MODELO_VIGENTE}/parametros_produccion.json"

# Mismo umbral que el proyecto original: por sobre este monto ya no es un
# arriendo real (venta mal clasificada o dato corrupto), así que el aviso se
# salta esta corrida en vez de predecir con un precio_clp implausible.
PRECIO_MAXIMO_ARRIENDO_CLP = 8_000_000

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Crear el esquema de Predicción y la tabla `predicciones` (si no existen)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gran_concepcion.04_prediccion")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.04_prediccion.predicciones (
        id_aviso              STRING NOT NULL,
        version_modelo        STRING NOT NULL,
        fecha_prediccion      TIMESTAMP NOT NULL,
        costo_total_predicho  DOUBLE NOT NULL,
        z_robusto             DOUBLE,
        decil_precio          INT,
        etiqueta              STRING,
        nivel_confianza       STRING,
        cv_ensamble           DOUBLE
    )
""")

print("Esquema y tabla predicciones verificados/creados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Cargar el modelo vigente y su calibración
# MAGIC `modelo_produccion.pkl` guarda `{"algoritmo": ..., "modelos": [...]}`: un
# MAGIC ensamble de modelos LightGBM entrenados con distintas semillas (bagging).
# MAGIC `parametros_produccion.json` trae la lista de features en el orden exacto
# MAGIC que el modelo espera, y la calibración de oportunidad/confianza
# MAGIC calculada en el test set al entrenar.

# COMMAND ----------

import json
import pickle

with open(RUTA_MODELO_PKL, "rb") as f:
    modelo_guardado = pickle.load(f)

with open(RUTA_PARAMETROS_JSON, encoding="utf-8") as f:
    parametros = json.load(f)

if modelo_guardado["algoritmo"] != "lightgbm":
    raise ValueError(
        f"Este notebook solo sabe predecir con LightGBM (via m.predict(..., num_iteration=m.best_iteration)); "
        f"el modelo cargado dice algoritmo={modelo_guardado['algoritmo']!r}. Si el modelo vigente cambió de "
        f"algoritmo, hay que ajustar la función de predicción de este notebook a mano."
    )

modelos_ensamble = modelo_guardado["modelos"]
version_modelo = parametros["version_modelo"]
features_modelo = parametros["features"]
calibracion = parametros["calibracion_oportunidad"]

print(f"Modelo vigente: {version_modelo} ({len(modelos_ensamble)} modelos en el ensamble)")
print(f"Features esperadas: {len(features_modelo)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Identificar avisos pendientes de predicción
# MAGIC Avisos de Oro que todavía no tienen una fila en `predicciones` para la
# MAGIC versión de modelo vigente (un reentrenamiento nuevo vuelve a predecir
# MAGIC todo el histórico bajo su propia versión, sin pisar las predicciones de
# MAGIC versiones anteriores).

# COMMAND ----------

pendientes_df = spark.sql(f"""
    SELECT f.*
    FROM gran_concepcion.03_oro.avisos_features f
    LEFT JOIN gran_concepcion.04_prediccion.predicciones p
        ON f.id_aviso = p.id_aviso AND p.version_modelo = '{version_modelo}'
    WHERE p.id_aviso IS NULL
""").toPandas()

print(f"{len(pendientes_df)} avisos pendientes de predicción para la versión {version_modelo}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Filtrar por precio máximo
# MAGIC `precio_clp` y `gastos_comunes` ya vienen calculados desde Plata/Oro (no
# MAGIC hace falta reconvertir UF→CLP, a diferencia del proyecto original, que sí
# MAGIC lo recalcula acá porque sus tablas de avisos en producción no guardan
# MAGIC `precio_clp`; el valor final es el mismo). Los avisos con `precio_clp`
# MAGIC sobre el umbral se saltan esta corrida — se reintentan en la próxima,
# MAGIC igual que en el original.

# COMMAND ----------

import numpy as np

pendientes_df["costo_total_real"] = pendientes_df["precio_clp"] + pendientes_df["gastos_comunes"]

mask_precio_valido = pendientes_df["precio_clp"].notna() & (pendientes_df["precio_clp"] <= PRECIO_MAXIMO_ARRIENDO_CLP)
saltados = int((~mask_precio_valido).sum())
pendientes_df = pendientes_df[mask_precio_valido].reset_index(drop=True)

print(f"{saltados} avisos saltados por precio_clp inválido o sobre el umbral. "
      f"{len(pendientes_df)} avisos siguen pendientes para predecir.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Predecir con el ensamble
# MAGIC Cada modelo del ensamble predice por separado (clip a `[0, ∞)`, un
# MAGIC arriendo no puede ser negativo); el promedio es la predicción final y la
# MAGIC desviación estándar entre modelos mide qué tan de acuerdo están entre sí
# MAGIC (`cv_ensamble`, la base del nivel de confianza).

# COMMAND ----------

if len(pendientes_df) == 0:
    print("No hay avisos pendientes de predicción esta corrida.")
else:
    X = pendientes_df.reindex(columns=features_modelo, fill_value=0).fillna(0)

    matriz_predicciones = np.stack([
        np.clip(m.predict(X, num_iteration=m.best_iteration), 0, None)
        for m in modelos_ensamble
    ])
    y_pred = matriz_predicciones.mean(axis=0)
    pred_std = matriz_predicciones.std(axis=0)
    cv_ensamble = pred_std / np.where(y_pred == 0, 1e-6, y_pred)

    pendientes_df["costo_total_predicho"] = y_pred
    pendientes_df["cv_ensamble"] = cv_ensamble

    print(f"Predicciones calculadas para {len(pendientes_df)} avisos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Calibración: decil de precio, z-score robusto, etiqueta y confianza
# MAGIC Usa los bordes de deciles y las medianas/MAD de error por decil ya
# MAGIC calculados al entrenar (guardados en `parametros_produccion.json`), en vez
# MAGIC de recalcularlos: con una sola fila nueva no se puede construir una
# MAGIC distribución.

# COMMAND ----------

import pandas as pd

if len(pendientes_df) > 0:
    bordes_deciles = calibracion["bordes_deciles_precio"]
    stats_por_decil = calibracion["stats_por_decil"]
    bordes_cv = calibracion["bordes_cv_confianza"]
    etiquetas_confianza = calibracion["etiquetas_confianza"]
    mad_scale = calibracion["mad_scale_const"]
    umbral_oportunidad = calibracion["umbral_oportunidad"]
    umbral_caro = calibracion["umbral_caro"]

    decil = pd.cut(pendientes_df["costo_total_real"], bins=bordes_deciles, labels=False, include_lowest=True)
    pendientes_df["decil_precio"] = decil.astype(int)

    idx_confianza = pd.cut(pendientes_df["cv_ensamble"], bins=bordes_cv, labels=False, include_lowest=True)
    pendientes_df["nivel_confianza"] = idx_confianza.map(lambda i: etiquetas_confianza[int(i)])

    z_robusto = []
    etiqueta = []
    for _, fila in pendientes_df.iterrows():
        stats_decil = stats_por_decil[str(int(fila["decil_precio"]))]
        mediana_decil = stats_decil["mediana_error"]
        mad_decil = stats_decil["mad_error"]
        mad_ajustado = mad_decil * mad_scale
        error = fila["costo_total_real"] - fila["costo_total_predicho"]
        z = (error - mediana_decil) / (mad_ajustado if mad_ajustado else 1e-6)
        z_robusto.append(z)

        if z < -umbral_oportunidad:
            etiqueta.append("oportunidad")
        elif z > umbral_caro:
            etiqueta.append("caro")
        else:
            etiqueta.append("precio_de_mercado")

    pendientes_df["z_robusto"] = z_robusto
    pendientes_df["etiqueta"] = etiqueta

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Armar DataFrame final y vista temporal

# COMMAND ----------

if len(pendientes_df) > 0:
    resultado = pendientes_df[[
        "id_aviso", "costo_total_predicho", "z_robusto", "decil_precio",
        "etiqueta", "nivel_confianza", "cv_ensamble",
    ]].copy()
    resultado["version_modelo"] = version_modelo

    spark.createDataFrame(resultado).createOrReplaceTempView("predicciones_nuevas_tmp")
    print(f"{len(resultado)} filas listas para el upsert.")
else:
    print("Nada que insertar esta corrida.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. MERGE hacia Predicción (upsert por `id_aviso` + `version_modelo`)
# MAGIC Re-correr esta etapa nunca duplica ni corrompe nada: un aviso ya
# MAGIC predicho con esta misma versión de modelo se actualiza en vez de
# MAGIC insertarse de nuevo.

# COMMAND ----------

if len(pendientes_df) > 0:
    spark.sql("""
        MERGE INTO gran_concepcion.04_prediccion.predicciones AS destino
        USING predicciones_nuevas_tmp AS nuevo
        ON destino.id_aviso = nuevo.id_aviso AND destino.version_modelo = nuevo.version_modelo
        WHEN MATCHED THEN UPDATE SET
            destino.fecha_prediccion = current_timestamp(),
            destino.costo_total_predicho = nuevo.costo_total_predicho,
            destino.z_robusto = nuevo.z_robusto,
            destino.decil_precio = nuevo.decil_precio,
            destino.etiqueta = nuevo.etiqueta,
            destino.nivel_confianza = nuevo.nivel_confianza,
            destino.cv_ensamble = nuevo.cv_ensamble
        WHEN NOT MATCHED THEN INSERT (
            id_aviso, version_modelo, fecha_prediccion, costo_total_predicho,
            z_robusto, decil_precio, etiqueta, nivel_confianza, cv_ensamble
        ) VALUES (
            nuevo.id_aviso, nuevo.version_modelo, current_timestamp(), nuevo.costo_total_predicho,
            nuevo.z_robusto, nuevo.decil_precio, nuevo.etiqueta, nuevo.nivel_confianza, nuevo.cv_ensamble
        )
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version_modelo, COUNT(*) AS predicciones, AVG(costo_total_predicho) AS costo_promedio
# MAGIC FROM gran_concepcion.04_prediccion.predicciones
# MAGIC GROUP BY version_modelo
# MAGIC ORDER BY version_modelo DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_aviso, costo_total_predicho, etiqueta, nivel_confianza, fecha_prediccion
# MAGIC FROM gran_concepcion.04_prediccion.predicciones
# MAGIC ORDER BY fecha_prediccion DESC
# MAGIC LIMIT 10
