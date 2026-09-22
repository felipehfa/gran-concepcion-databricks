# Databricks notebook source
# MAGIC %md
# MAGIC # 02, Scraper manual de detalle (Bronce)
# MAGIC
# MAGIC Visita individualmente cada aviso pendiente (los que ya están en la tabla
# MAGIC `avisos` pero todavía no tienen fila en `avisos_detalle`) y extrae su
# MAGIC descripción completa, características, coordenadas, estado de publicación y
# MAGIC puntos de interés cercanos, tal como los devuelve el sitio, en texto crudo,
# MAGIC sin transformar.
# MAGIC
# MAGIC El estado de publicación que se guarda acá (`avisos_detalle.estado_publicacion`)
# MAGIC es el que tenía el aviso la PRIMERA vez que se scrapeó su detalle, un dato
# MAGIC crudo más, igual que `descripcion` o `superficie_util_m2`, insertado una
# MAGIC sola vez y nunca actualizado después. El RE-CHEQUEO periódico de avisos ya
# MAGIC `activo` (para detectar si pasaron a pausado, finalizado, o si dejaron de
# MAGIC existir) no vive acá: se movió a
# MAGIC `03_oro/09_actualizacion_estado_avisos_oro_python.py`, que mantiene su
# MAGIC propia copia de `estado_publicacion` en Oro.
# MAGIC
# MAGIC **Bronce es 100% append-only en este notebook**: no hay ningún `UPDATE`
# MAGIC sobre filas ya insertadas. El control de la cola de scraping (qué avisos
# MAGIC reintentar, cuáles ya se saben inalcanzables) se resuelve con una tabla de
# MAGIC log append-only, `intentos_scraping_detalle` (una fila nueva por cada
# MAGIC intento de scrapeo, nunca se modifica una fila existente), en vez de
# MAGIC mutar contadores/columnas de estado en `avisos`.
# MAGIC
# MAGIC **Alcance de este notebook:**
# MAGIC - Corrida **manual**, sin scheduling.
# MAGIC - Extracción vía regex sobre el texto visible de la página, más un bloque
# MAGIC   JSON embebido para resolver puntos de interés y estado de publicación.
# MAGIC - Todo el scraping ocurre en **pandas puro**; Spark se usa para leer los
# MAGIC   pendientes (cruzando contra el log de intentos) y para el `MERGE INTO`
# MAGIC   final (upsert, solo inserta, nunca actualiza un aviso ya existente)
# MAGIC   hacia `avisos_detalle`.
# MAGIC - Se detiene de inmediato ante un CAPTCHA, dejando guardado en memoria lo ya
# MAGIC   procesado hasta ese punto, y activa un cooldown (tabla `control`) para que
# MAGIC   la próxima corrida no vuelva a intentar de inmediato.
# MAGIC - **Un tercer tipo de bloqueo, muro de verificación de cuenta del sitio**
# MAGIC   (redirige CUALQUIER URL a un login/verificación, con status 200 y sin
# MAGIC   CAPTCHA visible), se detecta aparte por la URL final tras redirects
# MAGIC   (`_es_muro_verificacion`) y se trata igual que un CAPTCHA: corta la
# MAGIC   corrida completa de inmediato, sin registrar ningún intento fallido y sin
# MAGIC   tocar ningún aviso. Mismo mecanismo que agregó el proyecto original tras
# MAGIC   un incidente real en agosto 2026 (ver su README, sección 9.5), donde este
# MAGIC   bloqueo, sin esta detección, hizo que ~1550 avisos activos se
# MAGIC   marcaran erróneamente como eliminados.
# MAGIC - Un aviso que falla la extracción de forma persistente (entre corridas, no
# MAGIC   dentro de la misma) sale de la cola de pendientes tras acumular
# MAGIC   `MAX_INTENTOS_FALLIDOS_DETALLE` intentos combinados de resultado `error` +
# MAGIC   `no_encontrado` (contados desde `intentos_scraping_detalle`, no desde un
# MAGIC   contador mutable). `no_encontrado` (la página descargada no corresponde al
# MAGIC   aviso pedido) ya NO excluye al aviso con un solo intento: ese mismatch
# MAGIC   puede deberse al muro de verificación de arriba en vez de a que el aviso
# MAGIC   específico fue eliminado, así que se trata con la misma cautela que un
# MAGIC   error de red.
# MAGIC
# MAGIC **Qué NO hace este notebook:**
# MAGIC - No transforma ningún valor, todo se guarda como texto, tal cual llega.
# MAGIC - No calcula distancias a puntos de referencia (eso ocurre en Oro).
# MAGIC - No resuelve columnas derivadas de fuentes externas (vulnerabilidad).
# MAGIC - No re-chequea avisos ya scrapeados para ver si cambiaron de estado (eso
# MAGIC   ocurre en `03_oro/09_actualizacion_estado_avisos_oro_python.py`).
# MAGIC - No actualiza ninguna fila ya insertada en Bronce.
# MAGIC
# MAGIC **Requisito previo:** debe existir al menos un aviso cargado en la tabla
# MAGIC `avisos` antes de correr este notebook (ver `01_scraper_manual_grilla_bronce`,
# MAGIC que también crea el esquema y las tablas si hace falta).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Importar librerías
# MAGIC Todas las librerías que usa este notebook, centralizadas al inicio. `beautifulsoup4` y `lxml` ya vienen declaradas como dependencias del entorno de este notebook (Databricks → Entorno del notebook), así que no hace falta un `%pip install` adicional; solo hay que importarlas.

# COMMAND ----------

import json
import random
import re
import time
import logging
import uuid
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# _id_corrida identifica todas las filas que esta ejecución del notebook
# escribe en Bronce (distinto de fecha_scrapeo, que es por fila).
ID_CORRIDA = str(uuid.uuid4())
SISTEMA_ORIGEN = "scraper_portalinmobiliario"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuración y constantes
# MAGIC Delays entre requests, headers, regex de extracción, selectores de
# MAGIC descripción, subcategorías de puntos de interés y radio máximo considerado
# MAGIC "cercano".

# COMMAND ----------

DELAY_MIN = 2.0
DELAY_MAX = 3.0
TIMEOUT_REQUEST_SEG = 2
REINTENTOS_TRAS_ERROR = 3
BACKOFF_REINTENTO_MIN = 3.0
BACKOFF_REINTENTO_MAX = 6.0

# Fallos de scraping CONSECUTIVOS (entre corridas, no dentro de la misma)
# antes de marcar un aviso como 'no_disponible' y sacarlo de la cola de
# pendientes nuevos.
MAX_INTENTOS_FALLIDOS_DETALLE = 5

# Tiempo mínimo de espera antes de reintentar tras un CAPTCHA, mismo valor
# que usa el scraper base del proyecto original.
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


def headers_requests(referer: str) -> dict:
    return {**HEADERS_BASE, "Referer": referer}


# El sitio, ante tráfico que clasifica como automatizado, puede redirigir
# CUALQUIER URL (incluida la portada) a un muro de login/verificación de
# cuenta en vez de devolver el contenido pedido - con status 200 y sin
# CAPTCHA visible, así que ni el chequeo de status code ni `hay_captcha` lo
# detectan. Se distingue por la URL final tras redirects (no por contenido
# del body), evaluado ANTES del chequeo de canonical/og:url: mismo criterio
# y mismas rutas que `scrapers_base/02_scraper_detalle.py` del proyecto
# original, que agregó esta detección tras un incidente real (agosto 2026,
# ver README sección 9.5 de ese proyecto) donde este mismo patrón - sin esta
# detección - hizo que ~1550 avisos activos se marcaran 'no_disponible' por
# error.
RUTAS_MURO_VERIFICACION = ("/gz/account-verification", "/jms/mlc/lgz/login", "/registration-pi")


def _es_muro_verificacion(url_final: str) -> bool:
    return any(ruta in url_final for ruta in RUTAS_MURO_VERIFICACION)


RE_FECHA_PUBLICACION = re.compile(r"Publicado (hoy|esta semana|hace [^\n\|]+)", re.IGNORECASE)
RE_SUPERFICIE_TOTAL = re.compile(r"Superficie total\s*([\d.,]+)\s*m", re.IGNORECASE)
RE_SUPERFICIE_UTIL = re.compile(r"Superficie útil\s*([\d.,]+)\s*m", re.IGNORECASE)
RE_DORMITORIOS = re.compile(r"Dormitorios\s*(\d+)")
RE_BANOS = re.compile(r"Baños\s*(\d+)")
RE_ESTACIONAMIENTOS = re.compile(r"Estacionamientos:?\s*(\d+)", re.IGNORECASE)
RE_ANTIGUEDAD = re.compile(r"Antigüedad\s*(\d+)\s*años?", re.IGNORECASE)
RE_AMOBLADO = re.compile(r"Amoblado:?\s*(Sí|No)", re.IGNORECASE)
RE_ADMITE_MASCOTAS = re.compile(r"Admite mascotas:?\s*(Sí|No)", re.IGNORECASE)
RE_CONDOMINIO_CERRADO = re.compile(r"En condominio cerrado:?\s*(Sí|No)", re.IGNORECASE)
RE_BODEGAS = re.compile(r"Bodegas\s*(\d+)", re.IGNORECASE)
RE_GASTOS_COMUNES = re.compile(r"Gastos comunes:?\s*\$?\s*([\d.,]+)", re.IGNORECASE)
RE_GASTOS_COMUNES_RESUMEN = re.compile(r"Gastos comunes\s+desde\s*\$?\s*([\d.,]+)", re.IGNORECASE)
RE_ESTACIONAMIENTO_VISITAS = re.compile(r"Estacionamiento de visitas:?\s*(Sí|No)", re.IGNORECASE)
RE_SOLO_FAMILIAS = re.compile(r"Solo familias:?\s*(Sí|No)", re.IGNORECASE)
RE_MAX_HABITANTES = re.compile(r"Cantidad máxima de habitantes\s*(\d+)", re.IGNORECASE)
RE_PISCINA = re.compile(r"Piscina:?\s*(Sí|No)", re.IGNORECASE)
RE_QUINCHO = re.compile(r"Quincho\D{0,15}?:?\s*(Sí|No)", re.IGNORECASE)
RE_CONSERJERIA = re.compile(r"Conserjería:?\s*(Sí|No)", re.IGNORECASE)
RE_ASCENSOR = re.compile(r"Ascensor:?\s*(Sí|No)", re.IGNORECASE)
RE_PISO_UNIDAD = re.compile(r"Número de piso de la unidad\s*(\d+)", re.IGNORECASE)
RE_DEPTOS_POR_PISO = re.compile(r"Departamentos por piso\s*(\d+)", re.IGNORECASE)
RE_LATITUD = re.compile(r'"latitude":"(-?[\d.]+)"')
RE_LONGITUD = re.compile(r'"longitude":"(-?[\d.]+)"')
RE_LATLON_MAPA = re.compile(r"center=(-?[\d.]+)%2C(-?[\d.]+)")

SELECTORES_DESCRIPCION = [
    "[data-testid='core-description'] p",
    "p.ui-pdp-description__content",
    "div.ui-pdp-description",
]

RADIO_MAXIMO_POI_M = 500

SUBCATEGORIAS_POI = {
    "paraderos": "paraderos",
    "estaciones de metro": "estaciones_metro",
    "jardines infantiles": "jardines_infantiles",
    "colegios": "colegios",
    "universidades": "universidades",
    "plazas": "plazas",
    "supermercados": "supermercados",
    "farmacias": "farmacias",
    "centros comerciales": "centros_comerciales",
    "hospitales": "hospitales",
    "clinicas": "clinicas",
}

DIAS_ESTA_SEMANA = 3

CLAVES_ESTADO_PUBLICACION = ("item_status_message", "item_status_short_description_message")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Crear `avisos_detalle` y `intentos_scraping_detalle` (si no existen)
# MAGIC Para que el pipeline se pueda reconstruir desde cero: si el catálogo está
# MAGIC vacío, deja creadas ambas tablas. Si ya existen, `IF NOT EXISTS` no hace
# MAGIC nada. El esquema de `avisos` y de `control` ya se crea en
# MAGIC `01_scraper_manual_grilla_bronce`.
# MAGIC
# MAGIC `avisos_detalle` incluye `estado_publicacion` (captura cruda de la primera
# MAGIC visita) y partición por `fecha_scrapeo`. `intentos_scraping_detalle` es un
# MAGIC log append-only, una fila por cada intento de scrapeo de detalle (nuevo o
# MAGIC reintento), nunca se actualiza una fila existente, que reemplaza los
# MAGIC contadores mutables que antes vivían en `avisos`.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.avisos_detalle (
        id_aviso                              STRING NOT NULL,
        descripcion                           STRING,
        fecha_publicacion_texto               STRING,
        fecha_publicacion_aprox               STRING,
        fecha_publicacion_precision           STRING,
        superficie_total_m2                   STRING,
        superficie_util_m2                    STRING,
        dormitorios                           STRING,
        banos                                 STRING,
        estacionamientos                      STRING,
        antiguedad_anos                       STRING,
        amoblado                              STRING,
        admite_mascotas                       STRING,
        condominio_cerrado                    STRING,
        bodegas                               STRING,
        gastos_comunes                        STRING,
        estacionamiento_visitas               STRING,
        solo_familias                         STRING,
        max_habitantes                        STRING,
        piscina                               STRING,
        quincho                               STRING,
        conserjeria                           STRING,
        ascensor                              STRING,
        piso_unidad                           STRING,
        deptos_por_piso                       STRING,
        barrio                                STRING,
        latitud                               STRING,
        longitud                              STRING,
        cantidad_paraderos                    STRING,
        distancia_min_m_paraderos             STRING,
        cantidad_estaciones_metro             STRING,
        distancia_min_m_estaciones_metro      STRING,
        cantidad_jardines_infantiles          STRING,
        distancia_min_m_jardines_infantiles   STRING,
        cantidad_colegios                     STRING,
        distancia_min_m_colegios              STRING,
        cantidad_universidades                STRING,
        distancia_min_m_universidades         STRING,
        cantidad_plazas                       STRING,
        distancia_min_m_plazas                STRING,
        cantidad_supermercados                STRING,
        distancia_min_m_supermercados         STRING,
        cantidad_farmacias                    STRING,
        distancia_min_m_farmacias             STRING,
        cantidad_centros_comerciales          STRING,
        distancia_min_m_centros_comerciales   STRING,
        cantidad_hospitales                   STRING,
        distancia_min_m_hospitales            STRING,
        cantidad_clinicas                     STRING,
        distancia_min_m_clinicas              STRING,
        estado_publicacion                    STRING,
        fecha_scrapeo                         STRING,
        _sistema_origen                        STRING,
        _id_corrida                            STRING
    )
    PARTITIONED BY (fecha_scrapeo)
""")

print("Tabla avisos_detalle verificada/creada.")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.intentos_scraping_detalle (
        id_aviso       STRING NOT NULL,
        resultado      STRING NOT NULL,
        motivo         STRING,
        fecha_intento  TIMESTAMP NOT NULL
    )
""")

print("Tabla intentos_scraping_detalle verificada/creada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Adaptador HTML
# MAGIC Envuelve el HTML descargado para que se comporte como la interfaz mínima
# MAGIC que necesitan las funciones de extracción (locator + inner_text).

# COMMAND ----------

class _LocatorHTMLEstatico:
    def __init__(self, soup, selector):
        self._soup = soup
        self._selector = selector

    @property
    def first(self):
        return self

    def count(self):
        if self._selector == "body":
            return 1
        return len(self._soup.select(self._selector))

    def inner_text(self):
        if self._selector == "body":
            nodo = self._soup.body or self._soup
        else:
            elementos = self._soup.select(self._selector)
            if not elementos:
                return ""
            nodo = elementos[0]
        return nodo.get_text("\n", strip=True)


class PaginaHTMLEstatico:
    def __init__(self, html):
        self._html = html or ""
        self._soup = BeautifulSoup(self._html, "lxml")

    def content(self):
        return self._html

    def locator(self, selector):
        return _LocatorHTMLEstatico(self._soup, selector)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 4. Funciones de parsing de fecha, descripción, coordenadas y JSON embebido
# MAGIC `parsear_fecha_relativa`/`determinar_precision_fecha` convierten el texto
# MAGIC relativo del sitio ("hoy", "esta semana", "hace 3 meses") a una fecha
# MAGIC absoluta y a un nivel de precisión. `extraer_descripcion` prueba varios
# MAGIC selectores CSS en orden (el sitio no siempre usa la misma clase).
# MAGIC `extraer_coordenadas` intenta primero el par `latitude`/`longitude` del
# MAGIC HTML crudo, y si no aparece, cae al parámetro `center=lat,lon` del mapa
# MAGIC embebido. `extraer_json_estado_pagina` aísla el bloque JSON `_n.ctx.r` que
# MAGIC trae, sin recortar, todas las categorías de puntos de interés y el estado
# MAGIC de publicación, la fuente que usan las funciones de la sección 5.

# COMMAND ----------

def parsear_fecha_relativa(texto):
    if not texto:
        return None
    texto_normalizado = texto.strip().lower()
    if texto_normalizado == "hoy":
        return date.today().isoformat()
    if texto_normalizado == "esta semana":
        return (date.today() - timedelta(days=DIAS_ESTA_SEMANA)).isoformat()

    m = re.search(r"(\d+)\s*(día|dias|semana|mes|meses|año|años)", texto, re.IGNORECASE)
    if not m:
        return None
    cantidad = int(m.group(1))
    unidad = m.group(2).lower()

    if "día" in unidad or "dia" in unidad:
        delta = timedelta(days=cantidad)
    elif "semana" in unidad:
        delta = timedelta(weeks=cantidad)
    elif "mes" in unidad:
        delta = timedelta(days=30 * cantidad)
    elif "año" in unidad:
        delta = timedelta(days=365 * cantidad)
    else:
        return None

    return (date.today() - delta).isoformat()


def determinar_precision_fecha(texto):
    if not texto:
        return None
    if texto.strip().lower() == "esta semana":
        return "aproximada_categoria"
    return "exacta"


def extraer_descripcion(page):
    for selector in SELECTORES_DESCRIPCION:
        loc = page.locator(selector)
        if loc.count() > 0:
            try:
                return loc.first.inner_text().strip()
            except Exception:
                continue
    return None


def extraer_coordenadas(page):
    html_completo = page.content()
    m_lat = RE_LATITUD.search(html_completo)
    m_lon = RE_LONGITUD.search(html_completo)
    if m_lat and m_lon:
        return {"latitud": m_lat.group(1), "longitud": m_lon.group(1)}
    m_mapa = RE_LATLON_MAPA.search(html_completo)
    if m_mapa:
        return {"latitud": m_mapa.group(1), "longitud": m_mapa.group(2)}
    return {"latitud": None, "longitud": None}


def normalizar_texto(texto):
    reemplazos = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n"}
    texto = texto.lower().strip()
    for con_tilde, sin_tilde in reemplazos.items():
        texto = texto.replace(con_tilde, sin_tilde)
    return texto


def parsear_distancia_metros(texto):
    m = re.search(r"([\d.,]+)\s*(metros|km)", texto, re.IGNORECASE)
    if not m:
        return None
    valor = float(m.group(1).replace(".", "").replace(",", "."))
    unidad = m.group(2).lower()
    return valor * 1000 if unidad == "km" else valor


def extraer_json_estado_pagina(page):
    html_completo = page.content()
    m = re.search(r"_n\.ctx\.r=(\{.*?\});_n\.ctx\.r\.assets\.manifest=", html_completo, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Búsqueda recursiva dentro del JSON embebido
# MAGIC Funciones auxiliares para ubicar, dentro del JSON de estado de la página,
# MAGIC las categorías de puntos de interés, valores por clave, y el componente de
# MAGIC estado de publicación.

# COMMAND ----------

def _buscar_categorias_poi(nodo):
    if isinstance(nodo, dict):
        categorias = nodo.get("categories")
        if isinstance(categorias, list) and categorias and isinstance(categorias[0], dict) \
                and "subcategories" in categorias[0]:
            return categorias
        for valor in nodo.values():
            resultado = _buscar_categorias_poi(valor)
            if resultado:
                return resultado
    elif isinstance(nodo, list):
        for item in nodo:
            resultado = _buscar_categorias_poi(item)
            if resultado:
                return resultado
    return None


def _buscar_valor_por_clave(nodo, clave):
    encontrados = []

    def _recorrer(n):
        if isinstance(n, dict):
            if clave in n:
                encontrados.append(n[clave])
            for v in n.values():
                _recorrer(v)
        elif isinstance(n, list):
            for item in n:
                _recorrer(item)

    _recorrer(nodo)
    for valor in encontrados:
        if valor:
            return valor
    return None


def _buscar_nodo_por_clave(nodo, claves):
    """
    Búsqueda recursiva del primer componente (dict) cuyo campo "id" sea
    alguna de `claves`, devolviendo el componente completo (no el valor de
    "id"). En el JSON real de la página cada componente es un dict con forma
    {"id": "item_status_message", "body": {...}, ...}, es decir
    "item_status_message" es el VALOR de la llave "id", no una llave propia
    del dict.
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


def extraer_puntos_interes(estado):
    resultado = {}
    for clave in SUBCATEGORIAS_POI.values():
        resultado[f"cantidad_{clave}"] = None
        resultado[f"distancia_min_m_{clave}"] = None

    if not estado:
        return resultado

    categorias = _buscar_categorias_poi(estado)
    if not categorias:
        return resultado

    for categoria in categorias:
        for subcategoria in categoria.get("subcategories", []):
            nombre = normalizar_texto(subcategoria.get("title", {}).get("text", ""))
            clave = SUBCATEGORIAS_POI.get(nombre)
            if not clave:
                continue

            items = subcategoria.get("items", [])
            distancias_dentro_del_radio = []
            for item in items:
                texto_subtitulo = item.get("subtitle", {}).get("label", {}).get("text", "")
                distancia = parsear_distancia_metros(texto_subtitulo)
                if distancia is not None and distancia <= RADIO_MAXIMO_POI_M:
                    distancias_dentro_del_radio.append(distancia)

            resultado[f"cantidad_{clave}"] = len(distancias_dentro_del_radio)
            resultado[f"distancia_min_m_{clave}"] = (
                min(distancias_dentro_del_radio) if distancias_dentro_del_radio else None
            )

    return resultado


def extraer_estado_publicacion(estado):
    """
    Busca, dentro de appProps.pageProps.initialState.components.head y
    .short_description, un componente item_status_message /
    item_status_short_description_message. Si no aparece -> 'activo'. Si
    aparece, clasifica su body.text: contiene "pausad" -> 'pausado',
    contiene "finaliz" -> 'finalizado', texto no reconocido -> 'pausado' por
    precaución (la sola presencia del componente ya señala que el aviso no
    está activo con normalidad).
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
    no a otra página (ej. el buscador) a la que el sitio a veces redirige
    cuando el aviso ya fue eliminado, sin devolver un status HTTP distinto de
    200. Exige que TANTO <link rel="canonical"> COMO <meta property="og:url">
    contengan `id_aviso`: son dos tags independientes del <head>, así que
    pedir que ambos coincidan reduce el riesgo de un falso positivo.
    """
    soup = BeautifulSoup(html or "", "lxml")

    canonical = soup.find("link", attrs={"rel": "canonical"})
    canonical_href = canonical.get("href", "") if canonical else ""

    og_url_tag = soup.find("meta", attrs={"property": "og:url"})
    og_url = og_url_tag.get("content", "") if og_url_tag else ""

    return id_aviso in canonical_href and id_aviso in og_url

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 6. Extracción completa de un aviso, y detección de CAPTCHA
# MAGIC `extraer_detalle` corre todas las regex de características (dormitorios,
# MAGIC baños, gastos comunes, amenities, etc.) sobre el texto visible de la
# MAGIC página, más las funciones de la sección 4/5 para fecha, coordenadas,
# MAGIC barrio, puntos de interés y estado de publicación, y arma un solo dict con
# MAGIC todos los campos crudos (sin castear nada). `hay_captcha` usa doble
# MAGIC condición a propósito: la palabra "captcha" en el HTML **y** ninguna señal
# MAGIC de contenido real (superficie/dormitorios), el reCAPTCHA de fondo casi
# MAGIC siempre está presente en el HTML aunque la página haya cargado bien, así
# MAGIC que exigir ambas condiciones evita falsos positivos. `construir_referer`
# MAGIC arma la URL de la página de búsqueda de origen, para que el request de
# MAGIC detalle no llegue "de la nada" a ojos del sitio.

# COMMAND ----------

def extraer_detalle(page):
    texto_completo = page.locator("body").inner_text()

    def buscar(patron):
        m = patron.search(texto_completo)
        return m.group(1).strip() if m else None

    gastos_comunes = buscar(RE_GASTOS_COMUNES)
    if gastos_comunes is None:
        gastos_comunes = buscar(RE_GASTOS_COMUNES_RESUMEN)

    fecha_texto = buscar(RE_FECHA_PUBLICACION)
    if fecha_texto and fecha_texto.lower().startswith("hace "):
        fecha_texto = fecha_texto[len("hace "):]

    coordenadas = extraer_coordenadas(page)
    estado = extraer_json_estado_pagina(page)
    puntos_interes = extraer_puntos_interes(estado)
    barrio = _buscar_valor_por_clave(estado, "neighborhood") if estado else None
    estado_publicacion = extraer_estado_publicacion(estado)

    return {
        "descripcion": extraer_descripcion(page),
        "fecha_publicacion_texto": fecha_texto,
        "fecha_publicacion_aprox": parsear_fecha_relativa(fecha_texto),
        "fecha_publicacion_precision": determinar_precision_fecha(fecha_texto),
        "superficie_total_m2": buscar(RE_SUPERFICIE_TOTAL),
        "superficie_util_m2": buscar(RE_SUPERFICIE_UTIL),
        "dormitorios": buscar(RE_DORMITORIOS),
        "banos": buscar(RE_BANOS),
        "estacionamientos": buscar(RE_ESTACIONAMIENTOS),
        "antiguedad_anos": buscar(RE_ANTIGUEDAD),
        "amoblado": buscar(RE_AMOBLADO),
        "admite_mascotas": buscar(RE_ADMITE_MASCOTAS),
        "condominio_cerrado": buscar(RE_CONDOMINIO_CERRADO),
        "bodegas": buscar(RE_BODEGAS),
        "gastos_comunes": gastos_comunes,
        "estacionamiento_visitas": buscar(RE_ESTACIONAMIENTO_VISITAS),
        "solo_familias": buscar(RE_SOLO_FAMILIAS),
        "max_habitantes": buscar(RE_MAX_HABITANTES),
        "piscina": buscar(RE_PISCINA),
        "quincho": buscar(RE_QUINCHO),
        "conserjeria": buscar(RE_CONSERJERIA),
        "ascensor": buscar(RE_ASCENSOR),
        "piso_unidad": buscar(RE_PISO_UNIDAD),
        "deptos_por_piso": buscar(RE_DEPTOS_POR_PISO),
        "latitud": coordenadas["latitud"],
        "longitud": coordenadas["longitud"],
        "barrio": barrio,
        "estado_publicacion": estado_publicacion,
        **puntos_interes,
    }


def hay_captcha(page):
    contenido = page.content().lower()
    if "captcha" not in contenido[:8000]:
        return False
    texto = page.locator("body").inner_text()
    parece_contenido_real = bool(
        RE_SUPERFICIE_TOTAL.search(texto)
        or RE_SUPERFICIE_UTIL.search(texto)
        or RE_DORMITORIOS.search(texto)
    )
    return not parece_contenido_real


def construir_referer(comuna, tipo_propiedad):
    return f"{BASE_URL}/{OPERACION}/{tipo_propiedad}/{comuna}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Descarga con reintento
# MAGIC Visita la URL de un aviso, con reintentos automáticos ante fallos
# MAGIC transitorios, y devuelve el resultado de la extracción completa.

# COMMAND ----------

def obtener_detalle_aviso(url, id_aviso, comuna, tipo_propiedad):
    """
    Devuelve "resultado": "ok" | "captcha" | "bloqueado" | "no_encontrado" | "error".

    "bloqueado" es distinto de "no_encontrado" aunque ambos se detectan sin
    un status HTTP distinto de 200: "bloqueado" significa que el SITIO
    ENTERO está redirigiendo a un muro de verificación (ver
    `_es_muro_verificacion`), no que este aviso en particular haya sido
    eliminado - se resuelve en el primer intento igual que un CAPTCHA
    (reintentar la misma URL no va a cambiar nada mientras el bloqueo esté
    activo) y el llamador debe cortar la corrida completa sin registrar
    ningún intento fallido para este aviso.

    "no_encontrado" es distinto de "error": el fetch funcionó (status 200,
    sin CAPTCHA, sin muro de verificación) pero la página descargada no
    corresponde a `id_aviso` (ver `_url_coincide_con_aviso`), típicamente
    porque el aviso ya no existe y el sitio redirigió a otra página sin
    devolver un status distinto de 200. No tiene sentido reintentar la misma
    URL, así que se resuelve en el primer intento, sin pasar por el loop de
    reintentos. Pero un mismatch de canonical/og:url puede tener otras
    causas además de "el aviso ya no existe" (ver incidente de agosto 2026
    en el proyecto original, que hizo que exactamente esta condición se
    disparara para TODOS los avisos por igual mientras el sitio bloqueaba
    todo el tráfico) - el llamador ya no lo trata como confirmación
    inmediata de eliminación, ver sección 8.
    """
    referer = construir_referer(comuna, tipo_propiedad)
    intentos_totales = 1 + REINTENTOS_TRAS_ERROR
    ultimo_status = None
    ultimo_motivo = None

    for intento in range(1, intentos_totales + 1):
        try:
            resp = requests.get(url, headers=headers_requests(referer), timeout=TIMEOUT_REQUEST_SEG)
            ultimo_status = resp.status_code

            if resp.status_code != 200:
                ultimo_motivo = f"status HTTP {resp.status_code}"
                raise ValueError(ultimo_motivo)

            if _es_muro_verificacion(resp.url):
                log.error(f"Muro de verificación de cuenta detectado al pedir {url} "
                          f"(redirigido a {resp.url}). Esto es el SITIO bloqueando tráfico "
                          f"automatizado en general, no que {id_aviso} en particular haya sido "
                          f"eliminado.")
                return {
                    "resultado": "bloqueado", "status_http": ultimo_status,
                    "motivo": f"redirigido a muro de verificación ({resp.url})",
                }

            pagina = PaginaHTMLEstatico(resp.text)

            if hay_captcha(pagina):
                return {"resultado": "captcha", "status_http": ultimo_status, "motivo": "captcha"}

            if not _url_coincide_con_aviso(resp.text, id_aviso):
                log.warning(f"La página descargada para {id_aviso} ({url}) no corresponde a ese aviso "
                            f"(canonical/og:url no lo mencionan), probablemente fue eliminado.")
                return {
                    "resultado": "no_encontrado", "status_http": ultimo_status,
                    "motivo": "la página devuelta no corresponde al aviso (probablemente eliminado)",
                }

            datos = extraer_detalle(pagina)
            return {"resultado": "ok", "datos": datos, "status_http": ultimo_status, "motivo": None}

        except (requests.RequestException, ValueError) as e:
            ultimo_motivo = ultimo_motivo or str(e)
            if intento < intentos_totales:
                espera = random.uniform(BACKOFF_REINTENTO_MIN, BACKOFF_REINTENTO_MAX)
                log.warning(f"Intento {intento}/{intentos_totales} falló para {url} ({ultimo_motivo}). "
                            f"Reintentando en {espera:.1f}s...")
                time.sleep(espera)
            else:
                log.warning(f"Agotados los {intentos_totales} intentos para {url} ({ultimo_motivo}).")

    return {"resultado": "error", "status_http": ultimo_status, "motivo": ultimo_motivo}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Funciones de persistencia incremental
# MAGIC Cooldown tras CAPTCHA (tabla `control`, sin cambios) y registro de cada
# MAGIC intento de scrapeo. A diferencia del diseño anterior, acá no hay ningún
# MAGIC `UPDATE`: cada intento (ok/error/no_encontrado) se **inserta** como una
# MAGIC fila nueva en `intentos_scraping_detalle`, fila por fila, a medida que se
# MAGIC visita cada aviso, en vez de esperar al final de la corrida, para que un
# MAGIC aviso ya intentado quede registrado aunque la corrida se corte después por
# MAGIC CAPTCHA.

# COMMAND ----------

def _sql_str(valor):
    """Escapa comillas simples para armar literales SQL seguros a partir de
    id_aviso (siempre con forma MLC-<dígitos>, pero se escapa igual por si acaso)."""
    return str(valor).replace("'", "''")


def leer_control(clave):
    fila = spark.sql(f"""
        SELECT valor FROM gran_concepcion.01_bronce.control WHERE clave = '{_sql_str(clave)}'
    """).collect()
    return fila[0]["valor"] if fila else None


def escribir_control(clave, valor):
    spark.sql(f"""
        MERGE INTO gran_concepcion.01_bronce.control AS c
        USING (SELECT '{_sql_str(clave)}' AS clave, '{_sql_str(valor)}' AS valor) AS nuevo
        ON c.clave = nuevo.clave
        WHEN MATCHED THEN UPDATE SET c.valor = nuevo.valor
        WHEN NOT MATCHED THEN INSERT (clave, valor) VALUES (nuevo.clave, nuevo.valor)
    """)


def tiempo_restante_cooldown():
    valor = leer_control("ultimo_captcha_detalle")
    if not valor:
        return None
    ultimo_captcha = datetime.fromisoformat(valor)
    transcurrido = datetime.now() - ultimo_captcha
    cooldown_total = timedelta(minutes=COOLDOWN_TRAS_CAPTCHA_MINUTOS)
    return (cooldown_total - transcurrido) if transcurrido < cooldown_total else None


def registrar_captcha():
    escribir_control("ultimo_captcha_detalle", datetime.now().isoformat())


def registrar_intento_scraping(id_aviso, resultado, motivo):
    """INSERT-only: nunca se actualiza una fila existente en Bronce, cada
    intento (nuevo o reintento) queda como una fila más en el log."""
    motivo_sql = f"'{_sql_str(motivo)}'" if motivo else "NULL"
    spark.sql(f"""
        INSERT INTO gran_concepcion.01_bronce.intentos_scraping_detalle
        VALUES ('{_sql_str(id_aviso)}', '{_sql_str(resultado)}', {motivo_sql}, current_timestamp())
    """)


def visitar_aviso(fila, resultados_acumulados):
    """
    Visita un aviso nuevo (sin detalle todavía). Cada resultado se registra
    de inmediato como una fila nueva en `intentos_scraping_detalle`
    (append-only, Bronce nunca se actualiza) - EXCEPTO "bloqueado", que no
    es un resultado propio de este aviso (ver más abajo). Devuelve 'ok',
    'error', 'no_encontrado', 'captcha' o 'bloqueado'. Si el resultado es
    exitoso, agrega la fila completa,incluido `estado_publicacion`, la
    captura cruda de esta primera visita, a `resultados_acumulados` para el
    MERGE final hacia `avisos_detalle`.

    HISTORIAL (por qué "no_encontrado" ya NO excluye al aviso de inmediato):
    hasta que se detectó el incidente de agosto 2026 en el proyecto original
    (ver su README, sección 9.5), un mismatch de canonical/og:url se asumía
    prueba suficiente de que el aviso fue eliminado. Esa asunción resultó
    falsa: un muro de verificación de cuenta del sitio dispara exactamente
    esa misma condición para TODOS los avisos por igual. Ahora ese caso
    (bloqueo de todo el sitio) se detecta aparte como "bloqueado" y corta la
    corrida sin registrar ningún intento; lo que sigue devolviendo
    "no_encontrado" ya descartó esa causa específica, pero se trata con la
    misma cautela que un "error" de red - ambos se registran en el log y la
    exclusión definitiva de `pendientes` se decide en la sección 10 contando
    intentos acumulados, no de un solo intento.
    """
    id_aviso = fila["id_aviso"]
    url = fila["url"]
    log.info(f"Visitando {id_aviso}: {url}")

    resultado = obtener_detalle_aviso(url, id_aviso, fila["comuna"], fila["tipo_propiedad"])

    if resultado["resultado"] == "captcha":
        log.error(f"CAPTCHA detectado en {url}. Deteniendo la corrida ahora mismo. "
                  f"Cooldown de {COOLDOWN_TRAS_CAPTCHA_MINUTOS} minutos antes de la próxima.")
        registrar_captcha()
        return "captcha"

    if resultado["resultado"] == "bloqueado":
        log.error(f"Muro de verificación detectado en {url} ({resultado['motivo']}). El sitio está "
                  f"bloqueando todo el tráfico automatizado, no es que {id_aviso} en particular haya "
                  f"sido eliminado. Deteniendo la corrida ahora mismo sin registrar ningún intento "
                  f"para este aviso. Mismo cooldown que un CAPTCHA "
                  f"({COOLDOWN_TRAS_CAPTCHA_MINUTOS} min) antes de reintentar.")
        registrar_captcha()
        return "bloqueado"

    if resultado["resultado"] == "no_encontrado":
        registrar_intento_scraping(id_aviso, "no_encontrado", resultado["motivo"])
        log.warning(f"{id_aviso}: la página descargada no corresponde a este aviso ({resultado['motivo']}). "
                    f"Se registra como intento fallido (cuenta para MAX_INTENTOS_FALLIDOS_DETALLE junto "
                    f"con los 'error', ver sección 10) en vez de excluirlo de inmediato.")
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        return "no_encontrado"

    if resultado["resultado"] == "error":
        registrar_intento_scraping(id_aviso, "error", resultado["motivo"])
        log.warning(f"No se pudo obtener {id_aviso} tras reintentos ({resultado['motivo']}).")
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        return "error"

    datos = dict(resultado["datos"])
    datos["id_aviso"] = id_aviso
    datos["fecha_scrapeo"] = date.today().isoformat()
    datos["_sistema_origen"] = SISTEMA_ORIGEN
    datos["_id_corrida"] = ID_CORRIDA
    resultados_acumulados.append(datos)

    registrar_intento_scraping(id_aviso, "ok", None)

    log.info(f"  -> Guardado. estado_publicacion={datos.get('estado_publicacion')}")
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    return "ok"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Chequear cooldown tras CAPTCHA
# MAGIC Si un CAPTCHA reciente dejó un cooldown activo (tabla `control`), el
# MAGIC notebook se detiene acá mismo sin visitar ningún aviso, para no reintentar
# MAGIC de inmediato y arriesgar otro bloqueo.

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
# MAGIC ### 10. Identificar avisos pendientes de detalle (nuevos)
# MAGIC LEFT JOIN entre `avisos` y `avisos_detalle` en Bronce: trae solo los avisos
# MAGIC que todavía no tienen su detalle scrapeado. Se excluyen los que acumulan
# MAGIC `MAX_INTENTOS_FALLIDOS_DETALLE` o más intentos `error`+`no_encontrado`
# MAGIC combinados en `intentos_scraping_detalle`, reemplaza el chequeo que antes
# MAGIC usaba las columnas mutables de `avisos`. `no_encontrado` ya no se excluye
# MAGIC con un solo intento (ver sección 8): un mismatch de canonical/og:url puede
# MAGIC deberse a un bloqueo temporal del sitio completo, no solo a que el aviso
# MAGIC específico fue eliminado.

# COMMAND ----------

pendientes_nuevos_rows = spark.sql(f"""
    SELECT a.id_aviso, a.url, a.comuna, a.tipo_propiedad
    FROM gran_concepcion.01_bronce.avisos a
    LEFT JOIN gran_concepcion.01_bronce.avisos_detalle d ON a.id_aviso = d.id_aviso
    LEFT JOIN (
        SELECT id_aviso, COUNT(*) AS fallos
        FROM gran_concepcion.01_bronce.intentos_scraping_detalle
        WHERE resultado IN ('error', 'no_encontrado')
        GROUP BY id_aviso
    ) f ON a.id_aviso = f.id_aviso
    WHERE d.id_aviso IS NULL
      AND a.url IS NOT NULL
      AND COALESCE(f.fallos, 0) <= {MAX_INTENTOS_FALLIDOS_DETALLE}
""").collect()

pendientes_nuevos = [row.asDict() for row in pendientes_nuevos_rows]
log.info(f"{len(pendientes_nuevos)} avisos nuevos pendientes de detalle.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Scraping de detalle, en memoria (nuevos)
# MAGIC Visita cada aviso nuevo pendiente, uno por uno, con delay aleatorio entre
# MAGIC requests. Se detiene de inmediato si detecta CAPTCHA. Las consecuencias de
# MAGIC cada resultado (estado de publicación, contador de fallos) se aplican
# MAGIC dentro de `visitar_aviso`, fila por fila.

# COMMAND ----------

resultados_nuevos = []
procesados_nuevos = 0
detenido_por_captcha = False

for fila in pendientes_nuevos:
    resultado = visitar_aviso(fila, resultados_acumulados=resultados_nuevos)

    if resultado in ("captcha", "bloqueado"):
        # "bloqueado" comparte la misma bandera/cooldown que "captcha":
        # ambos significan "el sitio nos está frenando a todos por igual
        # ahora mismo", así que la respuesta correcta es la misma (cortar
        # la corrida completa y esperar), aunque la causa raíz sea distinta
        # (ver visitar_aviso).
        detenido_por_captcha = True
        break
    if resultado == "ok":
        procesados_nuevos += 1

    log.info(f"  -> ({procesados_nuevos + 1}/{len(pendientes_nuevos)} intentados)")

log.info(f"Nuevos: procesados {procesados_nuevos} de {len(pendientes_nuevos)}. "
          f"Detenido por CAPTCHA: {detenido_por_captcha}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Armar DataFrame con los avisos nuevos
# MAGIC Sin ninguna conversión de tipo, todo queda como texto, tal cual llegó.

# COMMAND ----------

df_detalle = pd.DataFrame(resultados_nuevos)
print(f"{len(df_detalle)} filas para upsert")
df_detalle.head()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 13. Crear vista temporal para el upsert
# MAGIC Punto único de contacto con Spark: convierte `df_detalle` (pandas) en una
# MAGIC vista SQL temporal para que el `%sql MERGE` de la sección 14 la pueda
# MAGIC referenciar.
# MAGIC
# MAGIC Si no hay avisos nuevos (`df_detalle` vacío, ej. la cola de pendientes ya
# MAGIC estaba al día), `spark.createDataFrame(df_detalle)` no puede inferir un
# MAGIC esquema de un DataFrame de pandas vacío (`CANNOT_INFER_EMPTY_SCHEMA`). En
# MAGIC ese caso se arma la vista vacía con el esquema real de
# MAGIC `avisos_detalle` (`WHERE 1=0`) en vez de inferirlo, para que el `MERGE`
# MAGIC de la sección 14 quede como no-op en vez de romper la corrida.

# COMMAND ----------

if len(df_detalle) == 0:
    print("No hay avisos nuevos: vista temporal vacía, el MERGE de la sección 14 no insertará nada.")
    spark.sql("SELECT * FROM gran_concepcion.01_bronce.avisos_detalle WHERE 1 = 0") \
        .createOrReplaceTempView("detalle_upsert_tmp")
else:
    spark.createDataFrame(df_detalle).createOrReplaceTempView("detalle_upsert_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14. MERGE final hacia Bronce (upsert, solo inserta)
# MAGIC Inserta el detalle de los avisos nuevos, incluido `estado_publicacion`
# MAGIC (captura cruda de esta primera visita). No hay rama `WHEN MATCHED`: un
# MAGIC `id_aviso` que ya tiene fila en `avisos_detalle` nunca vuelve a pasar por
# MAGIC acá (la sección 10 ya lo excluye de "pendientes"), así que Bronce queda
# MAGIC estrictamente append-only.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gran_concepcion.01_bronce.avisos_detalle AS detalle
# MAGIC USING detalle_upsert_tmp AS nuevos
# MAGIC ON detalle.id_aviso = nuevos.id_aviso
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     id_aviso, descripcion, fecha_publicacion_texto, fecha_publicacion_aprox,
# MAGIC     fecha_publicacion_precision, superficie_total_m2, superficie_util_m2,
# MAGIC     dormitorios, banos, estacionamientos, antiguedad_anos,
# MAGIC     amoblado, admite_mascotas, condominio_cerrado, bodegas, gastos_comunes,
# MAGIC     estacionamiento_visitas, solo_familias, max_habitantes, piscina, quincho,
# MAGIC     conserjeria, ascensor, piso_unidad, deptos_por_piso, barrio,
# MAGIC     latitud, longitud,
# MAGIC     cantidad_paraderos, distancia_min_m_paraderos,
# MAGIC     cantidad_estaciones_metro, distancia_min_m_estaciones_metro,
# MAGIC     cantidad_jardines_infantiles, distancia_min_m_jardines_infantiles,
# MAGIC     cantidad_colegios, distancia_min_m_colegios,
# MAGIC     cantidad_universidades, distancia_min_m_universidades,
# MAGIC     cantidad_plazas, distancia_min_m_plazas,
# MAGIC     cantidad_supermercados, distancia_min_m_supermercados,
# MAGIC     cantidad_farmacias, distancia_min_m_farmacias,
# MAGIC     cantidad_centros_comerciales, distancia_min_m_centros_comerciales,
# MAGIC     cantidad_hospitales, distancia_min_m_hospitales,
# MAGIC     cantidad_clinicas, distancia_min_m_clinicas,
# MAGIC     estado_publicacion, fecha_scrapeo, _sistema_origen, _id_corrida
# MAGIC ) VALUES (
# MAGIC     nuevos.id_aviso, nuevos.descripcion, nuevos.fecha_publicacion_texto,
# MAGIC     nuevos.fecha_publicacion_aprox, nuevos.fecha_publicacion_precision,
# MAGIC     nuevos.superficie_total_m2, nuevos.superficie_util_m2,
# MAGIC     nuevos.dormitorios, nuevos.banos, nuevos.estacionamientos, nuevos.antiguedad_anos,
# MAGIC     nuevos.amoblado, nuevos.admite_mascotas, nuevos.condominio_cerrado,
# MAGIC     nuevos.bodegas, nuevos.gastos_comunes, nuevos.estacionamiento_visitas,
# MAGIC     nuevos.solo_familias, nuevos.max_habitantes, nuevos.piscina, nuevos.quincho,
# MAGIC     nuevos.conserjeria, nuevos.ascensor, nuevos.piso_unidad, nuevos.deptos_por_piso,
# MAGIC     nuevos.barrio, nuevos.latitud, nuevos.longitud,
# MAGIC     nuevos.cantidad_paraderos, nuevos.distancia_min_m_paraderos,
# MAGIC     nuevos.cantidad_estaciones_metro, nuevos.distancia_min_m_estaciones_metro,
# MAGIC     nuevos.cantidad_jardines_infantiles, nuevos.distancia_min_m_jardines_infantiles,
# MAGIC     nuevos.cantidad_colegios, nuevos.distancia_min_m_colegios,
# MAGIC     nuevos.cantidad_universidades, nuevos.distancia_min_m_universidades,
# MAGIC     nuevos.cantidad_plazas, nuevos.distancia_min_m_plazas,
# MAGIC     nuevos.cantidad_supermercados, nuevos.distancia_min_m_supermercados,
# MAGIC     nuevos.cantidad_farmacias, nuevos.distancia_min_m_farmacias,
# MAGIC     nuevos.cantidad_centros_comerciales, nuevos.distancia_min_m_centros_comerciales,
# MAGIC     nuevos.cantidad_hospitales, nuevos.distancia_min_m_hospitales,
# MAGIC     nuevos.cantidad_clinicas, nuevos.distancia_min_m_clinicas,
# MAGIC     nuevos.estado_publicacion, nuevos.fecha_scrapeo, nuevos._sistema_origen, nuevos._id_corrida
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15. Compactar (OPTIMIZE)
# MAGIC Cada corrida incremental agrega archivos chicos; se compactan
# MAGIC periódicamente para que las lecturas no se degraden con el tiempo.

# COMMAND ----------

spark.sql("OPTIMIZE gran_concepcion.01_bronce.avisos_detalle ZORDER BY (id_aviso)")