# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "beautifulsoup4",
#   "lxml",
# ]
# ///
# MAGIC %md
# MAGIC # 09 — Actualización de estado de avisos (Oro)
# MAGIC
# MAGIC Re-visita periódicamente los avisos que están `activo` en
# MAGIC `gran_concepcion.03_oro.avisos_features` para detectar si pasaron a
# MAGIC pausado, finalizado, o si dejaron de existir, y mantiene esa columna al
# MAGIC día en Oro.
# MAGIC
# MAGIC **Por qué en Oro y no en Bronce:** `01_bronce/02_scraper_manual_detalle_bronce_python.ipynb`
# MAGIC solo captura el estado de publicación tal como lo vio la PRIMERA vez que
# MAGIC scrapeó el detalle de un aviso — un dato crudo más, igual que
# MAGIC `descripcion` o `superficie_util_m2`. Como Plata y Oro son incrementales
# MAGIC (solo procesan avisos que todavía no existen aguas abajo), ese valor
# MAGIC crudo queda congelado en `avisos_limpios`/`avisos_features` desde el
# MAGIC momento en que el aviso entró a cada capa, y nunca se refresca. Este
# MAGIC notebook resuelve ese problema con el mismo patrón que ya usa
# MAGIC `07_vulnerabilidad_oro_python.py` (que también re-consulta una fuente
# MAGIC externa — ahí el shapefile, acá el sitio en vivo — para sobreescribir un
# MAGIC valor con el estado ACTUAL): re-chequea el sitio y sobreescribe
# MAGIC `estado_publicacion` en Oro, sin tocar el valor crudo que ya quedó
# MAGIC guardado en Bronce/Plata.
# MAGIC
# MAGIC Este notebook reemplaza la sección de re-chequeo que antes vivía dentro
# MAGIC de `02_scraper_manual_detalle_bronce_python.ipynb` (que ahora solo se
# MAGIC ocupa de la primera visita a cada aviso nuevo).
# MAGIC
# MAGIC **Qué recibe:** `gran_concepcion.03_oro.avisos_features` ya generada
# MAGIC (notebooks 06 y 07 ya corridos), con `estado_publicacion` y `url`
# MAGIC heredados de Plata (`url` viaja Bronce→Plata→Oro desde
# MAGIC `04_limpieza_plata_sql.py`) — este notebook no necesita leer nada de
# MAGIC Bronce directamente.
# MAGIC
# MAGIC **Qué entrega:** `estado_publicacion`, `fecha_chequeo_estado_oro` e
# MAGIC `intentos_fallidos_chequeo_estado_oro` actualizados en
# MAGIC `avisos_features`, fila por fila, a medida que se re-chequea cada aviso
# MAGIC (no en un solo `MERGE` al final): así un aviso ya re-chequeado en esta
# MAGIC corrida queda guardado aunque la corrida se corte después por un
# MAGIC CAPTCHA — mismo criterio de robustez que ya usa
# MAGIC `02_scraper_manual_detalle_bronce_python.ipynb`.
# MAGIC
# MAGIC **Incremental:** en cada corrida solo re-visita avisos `activo` cuyo
# MAGIC `fecha_chequeo_estado_oro` es NULL (nunca chequeados desde Oro) o superó
# MAGIC `DIAS_MIN_ENTRE_RECHEQUEOS` días, hasta un máximo de
# MAGIC `MAX_AVISOS_RECHEQUEO_POR_CORRIDA` por corrida. Un aviso que ya quedó
# MAGIC `pausado`, `finalizado` o `no_disponible` no se vuelve a chequear.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Importar librerías

# COMMAND ----------

import json
import random
import re
import time
import logging
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuración y constantes
# MAGIC Mismos valores que usaba la sección de re-chequeo que antes vivía en
# MAGIC `02_scraper_manual_detalle_bronce_python.ipynb`, solo que ahora aplican a
# MAGIC un notebook propio de Oro.

# COMMAND ----------

DELAY_MIN = 0.5
DELAY_MAX = 1.0
TIMEOUT_REQUEST_SEG = 1
REINTENTOS_TRAS_ERROR = 3
BACKOFF_REINTENTO_MIN = 3.0
BACKOFF_REINTENTO_MAX = 6.0

# Re-chequeo: cada cuántos días se vuelve a visitar un aviso `activo`, y
# cuántos re-chequeos como máximo por corrida.
DIAS_MIN_ENTRE_RECHEQUEOS = 1
MAX_AVISOS_RECHEQUEO_POR_CORRIDA = 200

# Fallos de re-chequeo CONSECUTIVOS (entre corridas) antes de marcar un aviso
# como 'no_disponible' y sacarlo de la cola de re-chequeo.
MAX_INTENTOS_FALLIDOS_CHEQUEO_ESTADO = 5

# Tiempo mínimo de espera antes de reintentar tras un CAPTCHA.
COOLDOWN_TRAS_CAPTCHA_MINUTOS = 60

BASE_URL = "https://www.portalinmobiliario.com"
OPERACION = "arriendo"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

HEADERS_BASE = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}


def headers_requests(referer):
    return {**HEADERS_BASE, "Referer": referer}


def construir_referer(comuna, tipo_propiedad):
    return f"{BASE_URL}/{OPERACION}/{tipo_propiedad}/{comuna}"


CLAVES_ESTADO_PUBLICACION = ("item_status_message", "item_status_short_description_message")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Asegurar que existan las columnas de re-chequeo en `avisos_features`
# MAGIC Si ya existen (de una corrida anterior), el `ALTER TABLE` falla
# MAGIC silenciosamente (se captura la excepción) — mismo patrón que usan
# MAGIC `05_imputacion_superficie_plata_python.py` y
# MAGIC `07_vulnerabilidad_oro_python.py`. `estado_publicacion` ya existe
# MAGIC (heredada de Plata); acá solo se agregan las columnas nuevas propias de
# MAGIC Oro.

# COMMAND ----------

columnas_a_asegurar = {
    "fecha_chequeo_estado_oro": "TIMESTAMP",
    "intentos_fallidos_chequeo_estado_oro": "INT",
}

for columna, tipo in columnas_a_asegurar.items():
    try:
        spark.sql(f"""
            ALTER TABLE gran_concepcion.03_oro.avisos_features
            ADD COLUMNS ({columna} {tipo})
        """)
        print(f"Columna '{columna}' agregada.")
    except Exception as e:
        print(f"Columna '{columna}' ya existía (o error menor): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Crear la tabla de control de Oro (si no existe)
# MAGIC Clave/valor genérica para estado interno de este notebook (cooldown tras
# MAGIC CAPTCHA). Propia de Oro — no se lee ni se escribe la tabla `control` de
# MAGIC Bronce, cada capa mantiene la suya.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.control (
        clave STRING NOT NULL,
        valor STRING
    )
""")

print("Tabla de control de Oro verificada/creada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Funciones de cooldown tras CAPTCHA

# COMMAND ----------

def _sql_str(valor):
    """Escapa comillas simples para armar literales SQL seguros a partir de
    id_aviso (siempre con forma MLC-<dígitos>, pero se escapa igual por si acaso)."""
    return str(valor).replace("'", "''")


def leer_control(clave):
    fila = spark.sql(f"""
        SELECT valor FROM gran_concepcion.03_oro.control WHERE clave = '{_sql_str(clave)}'
    """).collect()
    return fila[0]["valor"] if fila else None


def escribir_control(clave, valor):
    spark.sql(f"""
        MERGE INTO gran_concepcion.03_oro.control AS c
        USING (SELECT '{_sql_str(clave)}' AS clave, '{_sql_str(valor)}' AS valor) AS nuevo
        ON c.clave = nuevo.clave
        WHEN MATCHED THEN UPDATE SET c.valor = nuevo.valor
        WHEN NOT MATCHED THEN INSERT (clave, valor) VALUES (nuevo.clave, nuevo.valor)
    """)


def tiempo_restante_cooldown():
    valor = leer_control("ultimo_captcha_chequeo_estado_oro")
    if not valor:
        return None
    ultimo_captcha = datetime.fromisoformat(valor)
    transcurrido = datetime.now() - ultimo_captcha
    cooldown_total = timedelta(minutes=COOLDOWN_TRAS_CAPTCHA_MINUTOS)
    return (cooldown_total - transcurrido) if transcurrido < cooldown_total else None


def registrar_captcha():
    escribir_control("ultimo_captcha_chequeo_estado_oro", datetime.now().isoformat())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Funciones para determinar el estado de publicación
# MAGIC Mismas funciones que usa `02_scraper_manual_detalle_bronce_python.ipynb`
# MAGIC para esto (JSON embebido con el estado de la página, verificación de que
# MAGIC la página descargada corresponde al aviso, y detección de CAPTCHA),
# MAGIC copiadas acá porque los notebooks de este repo son autocontenidos (no
# MAGIC hay imports entre notebooks).

# COMMAND ----------

def extraer_json_estado_pagina(html_completo):
    m = re.search(r"_n\.ctx\.r=(\{.*?\});_n\.ctx\.r\.assets\.manifest=", html_completo, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def _buscar_nodo_por_clave(nodo, claves):
    """
    Búsqueda recursiva del primer componente (dict) cuyo campo "id" sea
    alguna de `claves`, devolviendo el componente completo (no el valor de
    "id").
    """
    if isinstance(nodo, dict):
        if nodo.get("id") in claves:
            return nodo
        for valor in nodo.values():
            resultado = _buscar_nodo_por_clave(valor, claves)
            if resultado is not None:
                return resultado
    elif isinstance(nodo, list):
        for item in nodo:
            resultado = _buscar_nodo_por_clave(item, claves)
            if resultado is not None:
                return resultado
    return None


def extraer_estado_publicacion(estado):
    """
    Busca, dentro de appProps.pageProps.initialState.components.head y
    .short_description, un componente item_status_message /
    item_status_short_description_message. Si no aparece -> 'activo'. Si
    aparece, clasifica su body.text: contiene "pausad" -> 'pausado',
    contiene "finaliz" -> 'finalizado', texto no reconocido -> 'pausado' por
    precaución.
    """
    if not estado or not isinstance(estado, dict):
        return "activo"

    initial_state = estado.get("appProps")
    initial_state = initial_state.get("pageProps") if isinstance(initial_state, dict) else None
    initial_state = initial_state.get("initialState") if isinstance(initial_state, dict) else None

    components = initial_state.get("components") \
        if isinstance(initial_state, dict) and isinstance(initial_state.get("components"), dict) \
        else {}
    subarboles = [components.get("head"), components.get("short_description")]

    for subarbol in subarboles:
        if not subarbol:
            continue
        componente = _buscar_nodo_por_clave(subarbol, CLAVES_ESTADO_PUBLICACION)
        if not componente:
            continue

        texto = ""
        if isinstance(componente, dict):
            body = componente.get("body")
            if isinstance(body, dict):
                texto = body.get("text") or ""

        texto_normalizado = texto.lower()
        if "pausad" in texto_normalizado:
            return "pausado"
        if "finaliz" in texto_normalizado:
            return "finalizado"

        log.warning(f"Estado de publicación con texto no reconocido: '{texto}'. Se marca 'pausado'.")
        return "pausado"

    return "activo"


def _url_coincide_con_aviso(html, id_aviso):
    """
    Confirma que la página descargada realmente corresponde a `id_aviso`, y
    no a otra página a la que el sitio a veces redirige cuando el aviso ya
    fue eliminado, sin devolver un status HTTP distinto de 200. Exige que
    TANTO <link rel="canonical"> COMO <meta property="og:url"> contengan
    `id_aviso`.
    """
    soup = BeautifulSoup(html or "", "lxml")

    canonical = soup.find("link", attrs={"rel": "canonical"})
    canonical_href = canonical.get("href", "") if canonical else ""

    og_url_tag = soup.find("meta", attrs={"property": "og:url"})
    og_url = og_url_tag.get("content", "") if og_url_tag else ""

    return id_aviso in canonical_href and id_aviso in og_url


def hay_captcha(html):
    """
    Chequeo liviano: este notebook no extrae superficie/dormitorios/baños
    (no hace falta para determinar el estado), así que a diferencia de
    Bronce no puede usarlos como señal de "contenido real". Alcanza con
    buscar la palabra "captcha" en el bloque inicial del HTML.
    """
    return "captcha" in (html or "").lower()[:8000]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Descarga con reintento y determinación de estado

# COMMAND ----------

def obtener_estado_aviso(url, id_aviso, comuna, tipo_propiedad):
    """
    Devuelve "resultado": "ok" | "captcha" | "no_encontrado" | "error".
    "no_encontrado" es distinto de "error": el fetch funcionó (status 200,
    sin CAPTCHA) pero la página descargada no corresponde a `id_aviso`,
    típicamente porque el aviso ya no existe. No tiene sentido reintentar la
    misma URL, así que se resuelve en el primer intento.
    """
    referer = construir_referer(comuna, tipo_propiedad)
    intentos_totales = 1 + REINTENTOS_TRAS_ERROR
    ultimo_motivo = None

    for intento in range(1, intentos_totales + 1):
        try:
            resp = requests.get(url, headers=headers_requests(referer), timeout=TIMEOUT_REQUEST_SEG)

            if resp.status_code != 200:
                ultimo_motivo = f"status HTTP {resp.status_code}"
                raise ValueError(ultimo_motivo)

            if hay_captcha(resp.text):
                return {"resultado": "captcha", "motivo": "captcha"}

            if not _url_coincide_con_aviso(resp.text, id_aviso):
                log.warning(f"La página descargada para {id_aviso} ({url}) no corresponde a ese aviso, "
                            f"probablemente fue eliminado.")
                return {
                    "resultado": "no_encontrado",
                    "motivo": "la página devuelta no corresponde al aviso (probablemente eliminado)",
                }

            estado = extraer_json_estado_pagina(resp.text)
            estado_publicacion = extraer_estado_publicacion(estado)
            return {"resultado": "ok", "estado_publicacion": estado_publicacion, "motivo": None}

        except (requests.RequestException, ValueError) as e:
            ultimo_motivo = ultimo_motivo or str(e)
            if intento < intentos_totales:
                espera = random.uniform(BACKOFF_REINTENTO_MIN, BACKOFF_REINTENTO_MAX)
                log.warning(f"Intento {intento}/{intentos_totales} falló para {url} ({ultimo_motivo}). "
                            f"Reintentando en {espera:.1f}s...")
                time.sleep(espera)
            else:
                log.warning(f"Agotados los {intentos_totales} intentos para {url} ({ultimo_motivo}).")

    return {"resultado": "error", "motivo": ultimo_motivo}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Funciones de persistencia incremental
# MAGIC Se aplican fila por fila, a medida que se re-chequea cada aviso, en vez
# MAGIC de esperar al final de la corrida: así un aviso ya re-chequeado queda
# MAGIC guardado aunque la corrida se corte después por CAPTCHA.

# COMMAND ----------

def actualizar_estado_publicacion_oro(id_aviso, estado_publicacion):
    spark.sql(f"""
        UPDATE gran_concepcion.03_oro.avisos_features
        SET estado_publicacion = '{_sql_str(estado_publicacion)}',
            fecha_chequeo_estado_oro = current_timestamp(),
            intentos_fallidos_chequeo_estado_oro = 0
        WHERE id_aviso = '{_sql_str(id_aviso)}'
    """)


def incrementar_intentos_fallidos_chequeo_estado_oro(id_aviso):
    spark.sql(f"""
        UPDATE gran_concepcion.03_oro.avisos_features
        SET intentos_fallidos_chequeo_estado_oro = COALESCE(intentos_fallidos_chequeo_estado_oro, 0) + 1,
            fecha_chequeo_estado_oro = current_timestamp()
        WHERE id_aviso = '{_sql_str(id_aviso)}'
    """)
    fila = spark.sql(f"""
        SELECT intentos_fallidos_chequeo_estado_oro FROM gran_concepcion.03_oro.avisos_features
        WHERE id_aviso = '{_sql_str(id_aviso)}'
    """).collect()
    return fila[0]["intentos_fallidos_chequeo_estado_oro"] if fila else 0


def visitar_aviso_oro(fila):
    """
    Re-chequea un aviso y aplica de inmediato las consecuencias de
    persistencia según el resultado. Devuelve 'ok', 'cambio_estado',
    'no_disponible', 'captcha' o 'error'.
    """
    id_aviso = fila["id_aviso"]
    url = fila["url"]
    log.info(f"Re-chequeando {id_aviso}: {url}")

    resultado = obtener_estado_aviso(url, id_aviso, fila["comuna"], fila["tipo_propiedad"])

    if resultado["resultado"] == "captcha":
        log.error(f"CAPTCHA detectado en {url}. Deteniendo la corrida ahora mismo. "
                  f"Cooldown de {COOLDOWN_TRAS_CAPTCHA_MINUTOS} minutos antes de la próxima.")
        registrar_captcha()
        return "captcha"

    if resultado["resultado"] == "no_encontrado":
        actualizar_estado_publicacion_oro(id_aviso, "no_disponible")
        log.warning(f"{id_aviso}: {resultado['motivo']}. Se marca estado_publicacion='no_disponible' de inmediato.")
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        return "no_disponible"

    if resultado["resultado"] == "error":
        nuevo_contador = incrementar_intentos_fallidos_chequeo_estado_oro(id_aviso)
        log.warning(f"No se pudo re-chequear {id_aviso} tras reintentos ({resultado['motivo']}). "
                    f"intentos_fallidos_chequeo_estado_oro={nuevo_contador}.")
        if nuevo_contador > MAX_INTENTOS_FALLIDOS_CHEQUEO_ESTADO:
            actualizar_estado_publicacion_oro(id_aviso, "no_disponible")
            log.warning(f"{id_aviso} superó MAX_INTENTOS_FALLIDOS_CHEQUEO_ESTADO="
                        f"{MAX_INTENTOS_FALLIDOS_CHEQUEO_ESTADO} fallos consecutivos. "
                        f"Se marca estado_publicacion='no_disponible'.")
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        return "error"

    estado_publicacion = resultado["estado_publicacion"]
    actualizar_estado_publicacion_oro(id_aviso, estado_publicacion)
    log.info(f"  -> estado_publicacion={estado_publicacion}")
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    return "ok" if estado_publicacion == "activo" else "cambio_estado"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Chequear cooldown tras CAPTCHA
# MAGIC Si un CAPTCHA reciente dejó un cooldown activo, el notebook se detiene
# MAGIC acá mismo sin visitar ningún aviso.

# COMMAND ----------

restante = tiempo_restante_cooldown()
if restante:
    minutos = int(restante.total_seconds() // 60) + 1
    log.warning(f"En cooldown tras un CAPTCHA reciente. Faltan ~{minutos} minutos. "
                f"No se hace nada en esta corrida.")
    dbutils.notebook.exit(f"Cooldown activo, faltan ~{minutos} minutos.")
else:
    log.info("Sin cooldown activo, se continúa con la corrida.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Identificar avisos pendientes de re-chequeo
# MAGIC Avisos `activo` en Oro cuyo `fecha_chequeo_estado_oro` es NULL (nunca
# MAGIC re-chequeados desde acá) o superó `DIAS_MIN_ENTRE_RECHEQUEOS` días.
# MAGIC `url` ya viene heredada de Plata en `avisos_features`, sin necesidad de
# MAGIC leer Bronce. Los NULL quedan primero (son los más urgentes).

# COMMAND ----------

pendientes_rows = spark.sql(f"""
    SELECT id_aviso, url, comuna, tipo_propiedad
    FROM gran_concepcion.03_oro.avisos_features
    WHERE estado_publicacion = 'activo'
      AND url IS NOT NULL
      AND (
          fecha_chequeo_estado_oro IS NULL
          OR fecha_chequeo_estado_oro <= current_timestamp() - INTERVAL {DIAS_MIN_ENTRE_RECHEQUEOS} DAYS
      )
    ORDER BY fecha_chequeo_estado_oro ASC NULLS FIRST
    LIMIT {MAX_AVISOS_RECHEQUEO_POR_CORRIDA}
""").collect()

pendientes = [row.asDict() for row in pendientes_rows]
log.info(f"{len(pendientes)} avisos activos pendientes de re-chequeo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Re-chequeo en memoria
# MAGIC Visita cada aviso pendiente, uno por uno, con delay aleatorio entre
# MAGIC requests. Se detiene de inmediato si detecta CAPTCHA. Las consecuencias
# MAGIC de cada resultado (estado de publicación, contador de fallos) se
# MAGIC aplican dentro de `visitar_aviso_oro`, fila por fila.

# COMMAND ----------

rechequeos_procesados = 0
cambios_estado = 0
detenido_por_captcha = False

for fila in pendientes:
    resultado = visitar_aviso_oro(fila)

    if resultado == "captcha":
        detenido_por_captcha = True
        break
    if resultado in ("ok", "cambio_estado"):
        rechequeos_procesados += 1
    if resultado in ("cambio_estado", "no_disponible"):
        cambios_estado += 1

log.info(f"Re-chequeo: procesados {rechequeos_procesados} de {len(pendientes)}. "
          f"Cambios de estado detectados: {cambios_estado}. Detenido por CAPTCHA: {detenido_por_captcha}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT estado_publicacion, COUNT(*) AS avisos
# MAGIC FROM gran_concepcion.03_oro.avisos_features
# MAGIC GROUP BY estado_publicacion
# MAGIC ORDER BY avisos DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_aviso, estado_publicacion, fecha_chequeo_estado_oro, intentos_fallidos_chequeo_estado_oro
# MAGIC FROM gran_concepcion.03_oro.avisos_features
# MAGIC WHERE fecha_chequeo_estado_oro IS NOT NULL
# MAGIC ORDER BY fecha_chequeo_estado_oro DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Compactar (OPTIMIZE)
# MAGIC Los `UPDATE` fila por fila de la sección 10 agregan archivos chicos; se
# MAGIC compactan periódicamente para que las lecturas no se degraden con el
# MAGIC tiempo.

# COMMAND ----------

spark.sql("OPTIMIZE gran_concepcion.03_oro.avisos_features ZORDER BY (id_aviso)")
