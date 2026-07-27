# gran-concepcion-databricks

## Descripción
Pipeline de Databricks que replica, capa por capa, el proyecto de scraping y
modelo de precios de arriendo del Gran Concepción: scraping de avisos,
limpieza, ingeniería de variables y predicción de precio, siguiendo la
arquitectura medallón de 3 capas (bronce, plata, oro).

## Índice
- [Cómo cumple el estándar medallón](#cómo-cumple-el-estándar-medallón)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Volumes necesarios](#volumes-necesarios)
- [Uso](#uso)
- [Requisitos](#requisitos)

## Cómo cumple el estándar medallón

Además de organizar el código en carpetas `01_bronce/02_plata/03_oro`, el
pipeline implementa los requisitos técnicos del patrón medallón (no solo la
convención de nombres):

| Requisito del estándar | Cómo se cumple acá |
|---|---|
| **Bronce append-only** — nunca se actualiza una fila ya insertada | `avisos`, `avisos_detalle` y `intentos_scraping_detalle` solo reciben `INSERT`. El control de reintentos del scraper (qué avisos reintentar, cuáles ya se saben inalcanzables) vive en `intentos_scraping_detalle` — un log append-only — en vez de en contadores/columnas mutables. Ver `01_bronce/02_scraper_manual_detalle_bronce_python.ipynb`. |
| **Gold solo lee de Silver**, nunca de Bronce | Todo `03_oro/` lee de `avisos_limpios` (Plata), con una única excepción documentada: `07_vulnerabilidad_oro_python.py` lee los polígonos IGVUST directo de Bronce, porque son una tabla de referencia estática sin ninguna transformación de Silver que aplicarles. |
| **Metadata de trazabilidad por fila** | `_sistema_origen` y `_id_corrida` (prefijo `_` para separarlas de las columnas de negocio) en las tablas de Bronce, heredadas por Plata y Oro. `_id_corrida` es un `uuid4()` compartido por todas las filas que escribe una misma ejecución del notebook. |
| **Dedup e incrementalidad** | `NOT EXISTS`/`spark.catalog.tableExists` para no reprocesar filas ya existentes, y `ROW_NUMBER() OVER (PARTITION BY id_aviso ...)` como red de seguridad ante corridas repetidas (`03_oro/06_features_oro_sql.py`). |
| **Particionado por capa** (fecha de ingesta en Bronce, fecha de negocio en Silver/Gold) | `PARTITIONED BY` en las tablas que crecen con cada corrida (`avisos`, `avisos_detalle`, `avisos_limpios`, `avisos_features`, `predicciones`). Las tablas de referencia estática (polígonos, población de referencia) no se particionan — son chicas y se sobreescriben completas. |
| **Compactación tras cada MERGE incremental** | Cada notebook automático termina con una celda `OPTIMIZE ... ZORDER BY (id_aviso)`. |
| **Una sola capa Gold**, sin capas extra fuera del modelo de 3 | La predicción de precio vive dentro de `03_oro/` (no en un esquema `04_prediccion` aparte): Oro es el layer de negocio completo — features + predicción/etiqueta lista para consumo. |
| **Convención de nombres** | `gran_concepcion.<capa>.<entidad>` (`01_bronce.avisos`, `02_plata.avisos_limpios`, `03_oro.avisos_features`, `03_oro.predicciones`); columnas de metadata siempre con prefijo `_`. |

Ver `CLAUDE.md` para el detalle de cada patrón (con secciones de código
citadas) y para los tradeoffs que se evaluaron antes de implementarlos.

## Estructura del proyecto
- `01_bronce/`: scraping (grilla y detalle) y carga de los polígonos de
  vulnerabilidad socioterritorial. Datos crudos, sin transformar, y
  100% append-only (el control de reintentos de scraping vive en un log
  append-only, `intentos_scraping_detalle`, no en columnas mutables).
- `02_plata/`: limpieza y tipado (números en formato chileno, conversión de
  precio a CLP, imputación de superficie con modelos ya entrenados).
- `03_oro/`: ingeniería de variables para el modelo (puntuando cada aviso
  contra la población de referencia congelada con la que se entrenó el
  modelo vigente), resolución de vulnerabilidad socioterritorial,
  actualización periódica del estado de publicación de los avisos
  (activo/pausado/finalizado/no_disponible), y la predicción de precio
  (ensamble LightGBM) con su calibración de oportunidad/confianza — Oro es
  el layer de negocio: features + predicción, lista para consumo.
- `subir_a_volumes/`: carpeta local de conveniencia (no versionada) con los
  archivos ya listos para subir a los Volumes — ver más abajo.

### Notebooks, en orden de ejecución

| # | Notebook | Tipo | Manual / automático |
|---|---|---|---|
| 1 | `01_bronce/00_carga_manual_poligonos_vulnerabilidad_bronce_python.py` | Python | Manual, una vez (o al actualizar el shapefile) |
| 2 | `01_bronce/01_scraper_manual_grilla_bronce_python.ipynb` | Python | Manual |
| 3 | `01_bronce/02_scraper_manual_detalle_bronce_python.ipynb` | Python | Manual |
| 4 | `02_plata/03_tasas_historicas_plata_python.py` | Python | Automático |
| 5 | `02_plata/04_limpieza_plata_sql.py` | SQL | Automático |
| 6 | `02_plata/05_imputacion_superficie_plata_python.py` | Python | Automático |
| 7 | `03_oro/00_carga_manual_poblacion_referencia_oro_python.py` | Python | Manual, una vez (o al reentrenar el modelo) |
| 8 | `03_oro/06_features_oro_sql.py` | SQL | Automático |
| 9 | `03_oro/07_vulnerabilidad_oro_python.py` | Python | Automático |
| 10 | `03_oro/09_actualizacion_estado_avisos_oro_python.py` | Python | Automático |
| 11 | `03_oro/10_prediccion_oro_python.py` | Python | Manual, una vez (o al reentrenar el modelo) |

Todos los notebooks son idempotentes: si se borran las tablas del catálogo
y se vuelve a correr todo en este orden, las tablas se recrean y se
repueblan solas, sin pasos manuales adicionales (salvo los dos notebooks
`00_carga_manual_...` y `10_prediccion`, que dependen de archivos subidos a
mano a un Volume — ver abajo).

## Volumes necesarios

Cuatro Volumes de Unity Catalog (dos de ellos en el esquema `03_oro`) para
los archivos que no vienen del scraper (modelos ya entrenados, shapefile,
dataset de referencia). Los archivos ya están preparados en
`subir_a_volumes/` — solo hay que subir el contenido de cada subcarpeta al
Volume correspondiente (UI de Databricks: Catalog Explorer → Volume →
Upload, o `databricks fs cp` / la CLI).

Crear los Volumes (esto también asegura que los esquemas existan; ejecutar
en un notebook SQL o en el editor de queries de Databricks):

```sql
CREATE SCHEMA IF NOT EXISTS gran_concepcion.01_bronce;
CREATE VOLUME IF NOT EXISTS gran_concepcion.01_bronce.vulnerabilidad;

CREATE SCHEMA IF NOT EXISTS gran_concepcion.02_plata;
CREATE VOLUME IF NOT EXISTS gran_concepcion.02_plata.modelos;

CREATE SCHEMA IF NOT EXISTS gran_concepcion.03_oro;
CREATE VOLUME IF NOT EXISTS gran_concepcion.03_oro.referencia_modelo;
CREATE VOLUME IF NOT EXISTS gran_concepcion.03_oro.modelo_prediccion;
```

| Volume | Ruta completa | Contenido (en `subir_a_volumes/...`) | Lo usa |
|---|---|---|---|
| `vulnerabilidad` | `/Volumes/gran_concepcion/01_bronce/vulnerabilidad/` | Shapefile IGVUST (`.shp .shx .dbf .prj`) — `01_bronce_vulnerabilidad/` | `01_bronce/00_carga_manual_poligonos_vulnerabilidad_bronce_python.py` |
| `modelos` | `/Volumes/gran_concepcion/02_plata/modelos/` | Modelos de imputación de superficie (`.pkl`) — `02_plata_modelos/` | `02_plata/05_imputacion_superficie_plata_python.py` |
| `referencia_modelo` | `/Volumes/gran_concepcion/03_oro/referencia_modelo/` | Dataset histórico, niveles de barrio, features seleccionadas, BD original (`.csv .json .db`) — `03_oro_referencia_modelo/` | `03_oro/00_carga_manual_poblacion_referencia_oro_python.py` |
| `modelo_prediccion` | `/Volumes/gran_concepcion/03_oro/modelo_prediccion/` | Ensamble LightGBM vigente (`.pkl .json`) — `03_oro_modelo_prediccion/` | `03_oro/10_prediccion_oro_python.py` |

> **Nota:** este Volume se renombró desde `gran_concepcion.04_prediccion.modelo`
> (la predicción se plegó dentro de Oro). Si ya tenías el modelo subido ahí,
> hay que crear el Volume nuevo y volver a subir `modelo_produccion.pkl` +
> `parametros_produccion.json` — no hay forma de "mover" un Volume entre
> esquemas, solo de resubir el contenido.

Ver `subir_a_volumes/README.md` para el detalle de cada archivo.

## Uso
1. Clona el repositorio.
2. Crea los 4 Volumes (sección anterior) y sube el contenido de
   `subir_a_volumes/` a cada uno.
3. Importa los notebooks a tu workspace de Databricks.
4. Corre los notebooks en el orden de la tabla de arriba.

## Requisitos
- Databricks Workspace con Unity Catalog habilitado (usa Volumes).
- Catálogo `gran_concepcion`.
- Acceso a internet desde el cluster/compute para el scraping y para
  consultar mindicador.cl (tasas de UF).

## Contribución
Si deseas contribuir, abre un issue o haz un pull request.

## Licencia
MIT
