# Graph Report - .  (2026-07-26)

## Corpus Check
- Corpus is ~11,542 words - fits in a single context window. You may not need a graph.

## Summary
- 34 nodes · 46 edges · 7 communities (4 shown, 3 thin omitted)
- Extraction: 74% EXTRACTED · 26% INFERRED · 0% AMBIGUOUS · INFERRED: 12 edges (avg confidence: 0.82)
- Token cost: 54,809 input · 0 output

## Community Hubs (Navigation)
- Configuracion del Bundle Databricks
- Capa de Prediccion (Oro -> Prediccion)
- Unity Catalog y Volumes
- Capa Bronce (scraping crudo)
- Capa Plata (limpieza y tipado)
- Capa Oro (features vs poblacion de referencia)
- Carpeta local subir_a_volumes

## God Nodes (most connected - your core abstractions)
1. `Arquitectura Medallón (Bronce/Plata/Oro + Predicción)` - 6 edges
2. `gran_concepcion (Unity Catalog catalog)` - 5 edges
3. `01_bronce/ (raw scraping + vulnerability polygons)` - 4 edges
4. `02_plata/ (cleaning, typing, surface imputation)` - 4 edges
5. `03_oro/ (feature engineering vs frozen reference population)` - 4 edges
6. `Target: dev (development, default)` - 4 edges
7. `gran-concepcion-databricks (project)` - 3 edges
8. `02_scraper_manual_detalle_bronce (notebook)` - 3 edges
9. `Volume: vulnerabilidad (01_bronce)` - 3 edges
10. `Volume: modelo (04_prediccion)` - 3 edges

## Surprising Connections (you probably didn't know these)
- `Target: dev (development, default)` --conceptually_related_to--> `gran-concepcion-databricks (project)`  [INFERRED]
  databricks.yml → README.md
- `Target: dev (development, default)` --conceptually_related_to--> `Databricks Workspace (Unity Catalog enabled)`  [INFERRED]
  databricks.yml → README.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Gran Concepción medallion pipeline execution order (10 notebooks)** — 01_bronce_00_carga_manual_poligonos_vulnerabilidad_bronce_python, 01_bronce_01_scraper_manual_grilla_bronce_python, 01_bronce_02_scraper_manual_detalle_bronce_python, 02_plata_03_tasas_historicas_plata_python, 02_plata_04_limpieza_plata_sql, 02_plata_05_imputacion_superficie_plata_python, 03_oro_00_carga_manual_poblacion_referencia_oro_python, 03_oro_06_features_oro_sql, 03_oro_07_vulnerabilidad_oro_python, 04_prediccion_08_prediccion_oro_python [EXTRACTED 1.00]
- **Unity Catalog Volumes supplying non-scraped artifacts (models, shapefile, reference dataset)** — readme_vulnerabilidad_volume, readme_modelos_volume, readme_referencia_modelo_volume, readme_modelo_volume [EXTRACTED 1.00]
- **Manual one-time execution notebooks (setup / retrain-triggered)** — 01_bronce_00_carga_manual_poligonos_vulnerabilidad_bronce_python, 03_oro_00_carga_manual_poblacion_referencia_oro_python, 04_prediccion_08_prediccion_oro_python [EXTRACTED 0.90]

## Communities (7 total, 3 thin omitted)

### Community 0 - "Configuracion del Bundle Databricks"
Cohesion: 0.33
Nodes (7): Databricks Asset Bundle: 'databricks', Databricks Bundles documentation, Databricks Asset Bundle (DAB) concept, Rationale: only 'dev' target defined; staging/prod optionally addable later, Target: dev (development, default), Databricks Workspace (Unity Catalog enabled), gran-concepcion-databricks (project)

### Community 1 - "Capa de Prediccion (Oro -> Prediccion)"
Cohesion: 0.33
Nodes (5): 04_prediccion/ (LightGBM ensemble inference + calibration), Arquitectura Medallón (Bronce/Plata/Oro + Predicción), LightGBM ensemble model (rent price prediction), Volume: modelo (04_prediccion), Rationale: replicate scraping+pricing pipeline layer by layer via medallion architecture

### Community 2 - "Unity Catalog y Volumes"
Cohesion: 0.33
Nodes (6): gran_concepcion (Unity Catalog catalog), IGVUST shapefile (socio-territorial vulnerability), Volume: modelos (02_plata), Volume: referencia_modelo (03_oro), Unity Catalog, Volume: vulnerabilidad (01_bronce)

### Community 3 - "Capa Bronce (scraping crudo)"
Cohesion: 0.50
Nodes (3): 01_scraper_manual_grilla_bronce (notebook), 02_scraper_manual_detalle_bronce (notebook), 01_bronce/ (raw scraping + vulnerability polygons)

## Knowledge Gaps
- **7 isolated node(s):** `subir_a_volumes/ (local staging folder for Volume uploads)`, `Unity Catalog`, `IGVUST shapefile (socio-territorial vulnerability)`, `LightGBM ensemble model (rent price prediction)`, `mindicador.cl (UF exchange-rate API)` (+2 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Arquitectura Medallón (Bronce/Plata/Oro + Predicción)` connect `Capa de Prediccion (Oro -> Prediccion)` to `Configuracion del Bundle Databricks`, `Capa Bronce (scraping crudo)`, `Capa Plata (limpieza y tipado)`, `Capa Oro (features vs poblacion de referencia)`?**
  _High betweenness centrality (0.446) - this node is a cross-community bridge._
- **Why does `gran-concepcion-databricks (project)` connect `Configuracion del Bundle Databricks` to `Capa de Prediccion (Oro -> Prediccion)`?**
  _High betweenness centrality (0.295) - this node is a cross-community bridge._
- **What connects `subir_a_volumes/ (local staging folder for Volume uploads)`, `Unity Catalog`, `IGVUST shapefile (socio-territorial vulnerability)` to the rest of the system?**
  _7 weakly-connected nodes found - possible documentation gaps or missing edges._