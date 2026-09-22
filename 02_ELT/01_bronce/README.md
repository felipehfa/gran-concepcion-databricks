# 02_ELT/01_bronce

Notebooks de proceso de la capa Bronce: scraping crudo de Portal Inmobiliario
y carga manual de la referencia de vulnerabilidad socioterritorial. Los DDL
de las tablas que pueblan estos notebooks viven en `01_DDL/01_bronce/`.

## Archivos

### `ELT_00_carga_manual_poligonos_vulnerabilidad_bronce_python`
Lee el shapefile IGVUST (Índice de Vulnerabilidad Socioterritorial por
Unidad Vecinal, basado en RSH) y lo escribe en `01_bronce.poligonos_vulnerabilidad_uv`.
Manual, se corre aproximadamente una vez al año cuando sale un shapefile
nuevo. **SCD2 append-only**: nunca actualiza una fila existente, cada
actualización agrega una fila nueva por `uv_rsh` (si algún atributo cambió),
marcada con `fecha_carga`. No forma parte del Job automático.

- **Volume que usa**: `gran_concepcion.01_bronce.vulnerabilidad` — ahí se
  sube a mano el shapefile (`.shp .shx .dbf .prj`) antes de correr el
  notebook.
- **Fuera de esta carpeta**: la tabla que llena la consume
  `02_ELT/03_oro/ELT_07_vulnerabilidad_oro_python` — única excepción del
  proyecto donde un notebook de Oro lee Bronce directo (los polígonos son
  referencia estática, sin transformación de Silver que aplicarles). Ese
  notebook filtra solo la versión vigente de cada UV (`QUALIFY ROW_NUMBER()
  ... ORDER BY fecha_carga DESC`).

### `ELT_01_scraper_manual_grilla_bronce_python`
Primera pasada del scraper: recorre las páginas de resultados de búsqueda de
Portal Inmobiliario (arriendo de departamentos, 10 comunas del Gran
Concepción) y guarda los avisos nuevos (solo `id_aviso`, título, precio,
ubicación, etc. — sin el detalle completo) en `01_bronce.avisos`. Corre
dentro del Job automático (cada 6h), primera tarea de la cadena. 100%
append-only.

- **Volume que usa**: ninguno.
- **Fuera de esta carpeta**: no lee nada fuera de Bronce. La tabla `avisos`
  que llena es leída por `ELT_02_scraper_manual_detalle_bronce_python` (para
  saber qué avisos todavía no tienen detalle) y por
  `02_ELT/02_silver/ELT_04_limpieza_plata_sql`.

### `ELT_02_scraper_manual_detalle_bronce_python`
Segunda pasada: visita individualmente cada aviso de `01_bronce.avisos` que
todavía no tiene fila en `01_bronce.avisos_detalle`, y extrae su descripción
completa, características, coordenadas y estado de publicación. El control
de reintentos (qué avisos reintentar, cuáles ya se saben inalcanzables) se
resuelve contra el log append-only `01_bronce.intentos_scraping_detalle`,
nunca con columnas mutables. Corre dentro del Job automático, justo después
del scraper de grilla.

- **Volume que usa**: ninguno.
- **Fuera de esta carpeta**: lee `01_bronce.avisos` (de
  `ELT_01_scraper_manual_grilla_bronce_python`) para saber los pendientes.
  La tabla `avisos_detalle` que llena la lee
  `02_ELT/02_silver/ELT_04_limpieza_plata_sql` para armar `avisos_limpios`.

## Notas
- Las 3 notebooks son estrictamente append-only (ningún `UPDATE` sobre filas
  ya insertadas), excepto que `ELT_00` ahora agrega versiones nuevas en vez
  de sobreescribir (ver SCD2 arriba).
- `_sistema_origen` y `_id_corrida` (UUID por corrida) están en todas las
  tablas que estos notebooks escriben — trazabilidad estándar de Bronce.
