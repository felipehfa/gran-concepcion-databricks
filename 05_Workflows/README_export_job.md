# Export del job `gran_concepcion_pipeline`

Snapshot de la configuración actual del job (generado el 2026-09-17 desde el
workspace `dbc-4028c9e3-aaab.cloud.databricks.com`, job_id `1060179417531534`)
para poder recrearlo en otra cuenta/workspace de Databricks.

## Contenido

`gran_concepcion_pipeline_job.json` — el cuerpo `settings` tal cual lo
devuelve `GET /api/2.1/jobs/get`, listo para usarse como body de
`POST /api/2.1/jobs/create` (o `databricks jobs create --json @archivo.json`).

## Pasos para recrearlo en otra cuenta

1. Clonar/copiar el repo del proyecto (`gran-concepcion-databricks`) al
   workspace destino, vía Databricks Repos o `databricks workspace import-dir`.
2. En `gran_concepcion_pipeline_job.json`, actualizar cada
   `tasks[].notebook_task.notebook_path` para que apunte a la ruta real donde
   quedaron los notebooks en el workspace destino (acá son rutas bajo
   `/Users/felipehfa@gmail.com/gran-concepcion-databricks/...`).
3. Actualizar `email_notifications.on_success` / `on_failure` con el email
   del usuario que va a ser dueño del job en la cuenta destino.
4. Correr `databricks jobs create --json @gran_concepcion_pipeline_job.json`
   apuntando al profile/workspace de la cuenta destino.
5. Revisar que el warehouse serverless / compute esté disponible — este job
   no fija `job_clusters` (corre notebooks en serverless), así que solo
   requiere que la cuenta destino tenga serverless habilitado para jobs.

## Requisitos previos que este export NO resuelve

Antes de que el job corra sin errores en la cuenta nueva, hace falta (ver
`README.md` del proyecto):

- Crear el catalog `gran_concepcion` y sus 3 esquemas (`01_bronce`,
  `02_plata`, `03_oro`) en Unity Catalog.
- Correr a mano los notebooks de carga "una vez"
  (`00_carga_manual_poligonos_vulnerabilidad_bronce_python`,
  `00_carga_manual_poblacion_referencia_oro_python`).
- Subir a un Volume el modelo entrenado (`.pkl`) y los archivos que usa
  `10_prediccion_oro_python` (ver `subir_a_volumes/README.md`).
