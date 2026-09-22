# 02_ELT/02_silver

Notebooks de proceso de la capa Plata (Silver): limpieza, tipado, conversión
de moneda e imputación de superficie. Los DDL de las tablas que pueblan
estos notebooks viven en `01_DDL/02_silver/`.

## Archivos

### `ELT_03_tasas_historicas_plata_python`
Descarga desde `mindicador.cl` las tasas históricas UF/USD y las cachea en
`02_plata.valores_pesos` (una fila por fecha). Corre **antes** que
`ELT_04_limpieza_plata_sql` dentro del Job automático — la conversión a CLP
de esa etapa necesita el cache ya poblado con las fechas de publicación de
los avisos pendientes.

- **Volume que usa**: ninguno (llama a la API pública de mindicador.cl).
- **Fuera de esta carpeta**: no lee tablas de otras capas. La tabla que
  llena la usa `ELT_04_limpieza_plata_sql`, misma carpeta.

### `ELT_04_limpieza_plata_sql`
Toma `01_bronce.avisos` + `01_bronce.avisos_detalle` (texto crudo, sin
transformar) y arma `02_plata.avisos_limpios`: números en formato chileno
convertidos a `DOUBLE`, precio convertido a CLP con la tasa UF/USD vigente
el día exacto de publicación del aviso (no la más reciente), y booleanos
normalizados a `INT`. Mantiene ambas versiones de cada campo (columna
`_texto` cruda + columna tipada) para trazabilidad.

- **Volume que usa**: ninguno.
- **Fuera de esta carpeta**: lee `01_bronce.avisos` y `01_bronce.avisos_detalle`
  (`02_ELT/01_bronce/`) y `02_plata.valores_pesos` (de `ELT_03`, esta misma
  carpeta). La tabla `avisos_limpios` que arma la consume
  `ELT_05_imputacion_superficie_plata_python` (misma carpeta) y
  `02_ELT/03_oro/ELT_06_features_oro_sql`.

### `ELT_05_imputacion_superficie_plata_python`
Para avisos sin `superficie_util_m2`/`superficie_total_m2` informada,
imputa el valor usando un modelo Random Forest ya entrenado (no se
reentrena acá), y marca `superficie_util_imputada`/`superficie_total_imputada`
para que Oro sepa distinguir un dato real de uno imputado. Actualiza
`02_plata.avisos_limpios` in place (MERGE).

- **Volume que usa**: `gran_concepcion.02_plata.modelos` — ahí está el
  `.pkl` del Random Forest de imputación, subido a mano (entrenado en el
  "proyecto original", este notebook solo lo carga con `joblib.load`).
- **Fuera de esta carpeta**: lee y escribe `02_plata.avisos_limpios` (de
  `ELT_04`, misma carpeta). El resultado lo consume
  `02_ELT/03_oro/ELT_06_features_oro_sql`, que es el primer notebook de Oro
  y arranca la cadena de feature engineering.

## Notas
- Orden de ejecución dentro del Job: `ELT_03` → `ELT_04` → `ELT_05` (los
  tres encadenados, sin nada de Oro en medio).
- A diferencia de Bronce, Plata sí actualiza filas existentes (`ELT_05` hace
  `MERGE` sobre `avisos_limpios`) — es la capa donde se corrige/completa el
  dato, no donde se preserva crudo.
