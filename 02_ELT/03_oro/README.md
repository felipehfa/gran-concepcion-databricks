# 02_ELT/03_oro

Notebooks de proceso de la capa Oro (Gold): feature engineering,
vulnerabilidad socioterritorial, actualización de estado, predicción de
precio y modelo dimensional Kimball. Los DDL de las tablas que pueblan
estos notebooks viven en `01_DDL/03_oro/`, las vistas de consumo que arman
los dos últimos notebooks viven en `03_Vistas/`.

## Archivos

### `ELT_00_carga_manual_poblacion_referencia_oro_python`
Carga el dataset histórico congelado (con el que se entrenó el modelo
vigente), los niveles de barrio y las features seleccionadas, y puebla
`03_oro.stg_poblacion_referencia`, `03_oro.dim_barrio`,
`03_oro.stg_features_seleccionadas`, `03_oro.stg_referencia_estadisticas` y
`03_oro.stg_referencia_estadisticas_por_comuna`. Manual, se corre una sola
vez (o al reentrenar el modelo). No forma parte del Job automático.

- **Volume que usa**: `gran_concepcion.03_oro.referencia_modelo` — dataset
  histórico, niveles de barrio, features seleccionadas y la BD original del
  proyecto original (`.csv .json .db`), subidos a mano.
- **Fuera de esta carpeta**: no lee nada de otras capas. Las tablas que
  puebla son la **población de referencia congelada** contra la que
  `ELT_06_features_oro_sql` (esta misma carpeta) puntúa cada aviso nuevo —
  nunca contra otros avisos que vayan llegando por el pipeline, para que el
  vector de features de un aviso no cambie según cuándo se corra el
  pipeline.

### `ELT_06_features_oro_sql`
Primer paso automático de Oro: toma `02_plata.avisos_limpios` y calcula las
features finales del modelo (antigüedad, vulnerabilidad con respaldo,
precio/m² de sector, distancias, etc.), puntuando cada aviso contra la
población de referencia congelada. Deja columnas de vulnerabilidad con un
valor de respaldo (media de la comuna) que `ELT_07` sobreescribe después con
el valor real. Escribe `03_oro.stg_avisos_features`.

- **Volume que usa**: ninguno.
- **Fuera de esta carpeta**: lee `02_plata.avisos_limpios`
  (`02_ELT/02_silver/`) y las tablas de referencia de `ELT_00` (esta misma
  carpeta). Corre justo después de `ELT_05_imputacion_superficie_plata_python`
  en el Job.

### `ELT_07_vulnerabilidad_oro_python`
Resuelve `uv_rsh` y los índices de vulnerabilidad reales para cada aviso,
cruzando su coordenada (latitud/longitud) contra los polígonos de Unidad
Vecinal mediante punto-en-polígono con `shapely`. Sobreescribe el valor de
respaldo que dejó `ELT_06`. Incremental: solo procesa avisos con `uv_rsh`
todavía NULL; una vez resuelto, el aviso queda congelado aunque el polígono
reciba una versión nueva después (mismo principio que la población de
referencia congelada).

- **Volume que usa**: ninguno directamente, pero depende de que
  `02_ELT/01_bronce/ELT_00_carga_manual_poligonos_vulnerabilidad_bronce_python`
  ya haya subido el Volume `gran_concepcion.01_bronce.vulnerabilidad` y
  poblado la tabla.
- **Fuera de esta carpeta**: es el único notebook de Oro que lee una tabla
  de Bronce directo, `01_bronce.poligonos_vulnerabilidad_uv`
  (`02_ELT/01_bronce/`) — excepción deliberada porque es referencia
  estática sin transformación de Silver. Filtra solo la versión vigente de
  cada UV (SCD2 append-only, `QUALIFY ROW_NUMBER() ... ORDER BY fecha_carga
  DESC`). Actualiza `03_oro.stg_avisos_features` (de `ELT_06`).

### `ELT_09_actualizacion_estado_avisos_oro_python`
Re-chequea periódicamente los avisos con `estado_publicacion = activo`,
visitando de nuevo la URL del aviso en Portal Inmobiliario, para detectar si
pasaron a pausado, finalizado o dejaron de existir. El estado capturado la
primera vez en Bronce (`avisos_detalle.estado_publicacion`) nunca se
actualiza ahí; Oro es la fuente de verdad del estado de publicación para el
resto del pipeline.

- **Volume que usa**: ninguno.
- **Fuera de esta carpeta**: usa `url` (que viajó Bronce→Plata→Oro a través
  de `02_ELT/02_silver/ELT_04_limpieza_plata_sql`) para volver a visitar el
  sitio. Actualiza `03_oro.stg_avisos_features`.

### `ELT_10_prediccion_oro_python`
Carga el ensamble LightGBM ya entrenado y predice `costo_total_predicho`,
`z_robusto`, `decil_precio`, `etiqueta` (oportunidad o no) y
`nivel_confianza` para cada aviso con features completas. Escribe
`03_oro.stg_predicciones` (`MERGE` por `id_aviso` + `version_modelo`, nunca
duplica ni corrompe si se re-corre). Este notebook no entrena, solo carga y
predice.

- **Volume que usa**: `gran_concepcion.03_oro.modelo_prediccion` — el
  ensamble LightGBM vigente (`.pkl .json`), subido a mano, entrenado en el
  "proyecto original".
- **Fuera de esta carpeta**: lee `03_oro.stg_avisos_features` (de `ELT_06`/
  `ELT_07`, esta misma carpeta). El resultado lo consume
  `ELT_11_modelo_dimensional_oro_sql` para armar `fact_aviso` y
  `dim_prediccion_scd2`.

### `ELT_11_modelo_dimensional_oro_sql`
Arma el modelo dimensional Kimball: `03_oro.dim_descripcion_propiedad`,
`dim_amenidades`, `dim_ubicacion` (+ `dim_barrio`), `dim_tiempo`,
`dim_estado_aviso_scd2`, `dim_prediccion_scd2` (las dos últimas SCD2, con
histórico) y `03_oro.fact_aviso` (grano 1 fila por aviso vigente, 6 FKs, sin
hub `dim_propiedad`). También crea las vistas de consumo `vw_buscador_*` en
el schema `04_capa_semantica` (archivos de referencia en `03_Vistas/`).

- **Volume que usa**: ninguno.
- **Fuera de esta carpeta**: lee `03_oro.stg_avisos_features` y
  `03_oro.stg_predicciones` (`ELT_06`/`ELT_07`/`ELT_10`, esta misma
  carpeta) y las tablas de `ELT_00` (`dim_barrio`). Las vistas que crea las
  usa el dashboard AI/BI en `04_Visualización/`.

### `ELT_12_snapshot_historial_tablas_oro_python`
Última tarea del Job: registra un snapshot de `DESCRIBE HISTORY` de todas
las tablas Delta de las 3 capas en `03_oro.historial_tablas` (append-only —
necesario porque el log Delta se poda a ~30 días), y arma las vistas de
observabilidad `vw_corridas*` en `04_capa_semantica` que alimentan la
página "⚙️ Corridas" del dashboard.

- **Volume que usa**: ninguno.
- **Fuera de esta carpeta**: lee el historial Delta de tablas en
  `02_ELT/01_bronce/`, `02_ELT/02_silver/` y esta misma carpeta (cruza
  contra `system.lakeflow.job_run_timeline`/`job_task_run_timeline` para
  reconstruir avisos nuevos, rechequeos y predicciones por corrida). Las
  vistas que crea las usa el dashboard en `04_Visualización/`.

## Notas
- Orden dentro del Job: `ELT_06` → `ELT_07` → `ELT_09` → `ELT_10` → `ELT_11`
  → `ELT_12` (después de `ELT_05_imputacion_superficie_plata_python` de
  `02_ELT/02_silver/`).
- `ELT_00` es la única manual/fuera del Job en esta carpeta.
- De los 3 Volumes que toca esta carpeta y la de Bronce en conjunto, Oro usa
  2: `referencia_modelo` (población congelada) y `modelo_prediccion`
  (ensamble LightGBM) — ambos leídos, ninguno escrito por estos notebooks.
