# gran-concepcion-databricks

## Descripción
Pipeline de Databricks que replica, capa por capa, el proyecto de scraping y
modelo de precios de arriendo del Gran Concepción: scraping de avisos,
limpieza, ingeniería de variables y predicción de precio, siguiendo la
arquitectura medallón (bronce, plata, oro) más una capa de predicción.

## Estructura del proyecto
- `01_bronce/`: scraping (grilla y detalle) y carga de los polígonos de
  vulnerabilidad socioterritorial. Datos crudos, sin transformar.
- `02_plata/`: limpieza y tipado (números en formato chileno, conversión de
  precio a CLP, imputación de superficie con modelos ya entrenados).
- `03_oro/`: ingeniería de variables para el modelo, puntuando cada aviso
  contra la población de referencia congelada con la que se entrenó el
  modelo vigente, y resolución de vulnerabilidad socioterritorial.
- `04_prediccion/`: inferencia con el modelo (ensamble LightGBM) y
  calibración de oportunidad/confianza.
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
| 10 | `04_prediccion/08_prediccion_oro_python.py` | Python | Manual, una vez (o al reentrenar el modelo) |

Todos los notebooks son idempotentes: si se borran las tablas del catálogo
y se vuelve a correr todo en este orden, las tablas se recrean y se
repueblan solas, sin pasos manuales adicionales (salvo los dos notebooks
`00_carga_manual_...` y `08_prediccion`, que dependen de archivos subidos a
mano a un Volume — ver abajo).

## Volumes necesarios

Cuatro Volumes de Unity Catalog, uno por esquema, para los archivos que no
vienen del scraper (modelos ya entrenados, shapefile, dataset de
referencia). Los archivos ya están preparados en `subir_a_volumes/` — solo
hay que subir el contenido de cada subcarpeta al Volume correspondiente
(UI de Databricks: Catalog Explorer → Volume → Upload, o `databricks fs cp`
/ la CLI).

Crear los Volumes (esto también asegura que los esquemas existan; ejecutar
en un notebook SQL o en el editor de queries de Databricks):

```sql
CREATE SCHEMA IF NOT EXISTS gran_concepcion.01_bronce;
CREATE VOLUME IF NOT EXISTS gran_concepcion.01_bronce.vulnerabilidad;

CREATE SCHEMA IF NOT EXISTS gran_concepcion.02_plata;
CREATE VOLUME IF NOT EXISTS gran_concepcion.02_plata.modelos;

CREATE SCHEMA IF NOT EXISTS gran_concepcion.03_oro;
CREATE VOLUME IF NOT EXISTS gran_concepcion.03_oro.referencia_modelo;

CREATE SCHEMA IF NOT EXISTS gran_concepcion.04_prediccion;
CREATE VOLUME IF NOT EXISTS gran_concepcion.04_prediccion.modelo;
```

| Volume | Ruta completa | Contenido (en `subir_a_volumes/...`) | Lo usa |
|---|---|---|---|
| `vulnerabilidad` | `/Volumes/gran_concepcion/01_bronce/vulnerabilidad/` | Shapefile IGVUST (`.shp .shx .dbf .prj`) — `01_bronce_vulnerabilidad/` | `01_bronce/00_carga_manual_poligonos_vulnerabilidad_bronce_python.py` |
| `modelos` | `/Volumes/gran_concepcion/02_plata/modelos/` | Modelos de imputación de superficie (`.pkl`) — `02_plata_modelos/` | `02_plata/05_imputacion_superficie_plata_python.py` |
| `referencia_modelo` | `/Volumes/gran_concepcion/03_oro/referencia_modelo/` | Dataset histórico, niveles de barrio, features seleccionadas, BD original (`.csv .json .db`) — `03_oro_referencia_modelo/` | `03_oro/00_carga_manual_poblacion_referencia_oro_python.py` |
| `modelo` | `/Volumes/gran_concepcion/04_prediccion/modelo/` | Ensamble LightGBM vigente (`.pkl .json`) — `04_prediccion_modelo/` | `04_prediccion/08_prediccion_oro_python.py` |

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
