# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # 01, Scraper manual de grilla (Bronce)
# MAGIC
# MAGIC Recorre las páginas de resultados de búsqueda (grilla) para un conjunto de
# MAGIC comunas y tipos de propiedad, extrayendo los datos básicos de cada aviso
# MAGIC publicado, como título, precio, ubicación, dormitorios, baños, superficie y
# MAGIC guardando solo los avisos nuevos en la tabla `avisos` de Bronce.
# MAGIC
# MAGIC **Alcance de este notebook:**
# MAGIC - Corrida **manual**, sin scheduling, se ejecuta a mano cuando se quiera
# MAGIC   cargar avisos nuevos.
# MAGIC - Extracción vía selectores CSS y regex sobre el texto de cada tarjeta de
# MAGIC   resultado.
# MAGIC - Todo el scraping ocurre en **pandas puro** (sin la API de DataFrames de
# MAGIC   Spark); Spark se usa únicamente para leer los IDs ya existentes y para el
# MAGIC   `MERGE INTO` final hacia la tabla Delta.
# MAGIC - Corta la búsqueda de una comuna/tipo tras varias páginas seguidas sin
# MAGIC   avisos nuevos, y corta la corrida completa si se supera un techo de
# MAGIC   páginas o de tiempo, para no scrapear sin límite en una sola ejecución.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Importar librerías
# MAGIC Todas las librerías que usa este notebook, centralizadas al inicio. `beautifulsoup4` y `lxml` ya vienen declaradas como dependencias del entorno de este notebook (Databricks → Entorno del notebook), así que no hace falta un `%pip install` adicional; solo hay que importarlas.

# COMMAND ----------

# MAGIC %pip install beautifulsoup4 lxml
# MAGIC %restart_python

# COMMAND ----------

import logging
import random
import re
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# _id_corrida identifica todas las filas que esta ejecución del notebook
# escribe en Bronce (distinto de first_seen, que es por fila).
ID_CORRIDA = str(uuid.uuid4())
SISTEMA_ORIGEN = "scraper_portalinmobiliario"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Verificar salida a internet
# MAGIC Confirma que el compute puede alcanzar el sitio antes de correr el resto del
# MAGIC notebook, si esto falla, no tiene sentido seguir.

# COMMAND ----------

resp = requests.get("https://www.portalinmobiliario.com", timeout=15)

if resp.status_code == 200:
    print("Conección exitosa!")
else:
    print("Error!")


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 2. Configuración: comunas, límites de la corrida y selectores de scraping
# MAGIC Las 10 comunas y el tipo de propiedad (`departamento`, `arriendo`) que
# MAGIC procesa todo el pipeline; los techos que acotan cuánto scrapea una corrida
# MAGIC (páginas totales, páginas vacías consecutivas, minutos); los `HEADERS` y
# MAGIC `DELAY_MIN`/`DELAY_MAX` para no golpear el sitio como un bot; los
# MAGIC selectores CSS (`SELECTORES`, con alternativas por si el sitio cambia de
# MAGIC clase) y las regex (`RE_DORMITORIOS`/`RE_BANOS`/`RE_M2`) que se usan más
# MAGIC abajo para parsear cada tarjeta de resultado; y el dataclass `Aviso`, que
# MAGIC define la forma de una fila antes de convertirla a dict e insertarla.

# COMMAND ----------

COMUNAS_GRAN_CONCEPCION = [
    "concepcion-biobio",
    "talcahuano-biobio",
    "hualpen-biobio",
    "san-pedro-de-la-paz-biobio",
    "chiguayante-biobio",
    "penco-biobio",
    "tome-biobio",
    "coronel-biobio",
    "hualqui-biobio",
    "lota-biobio",
]

# Solo departamento, igual que el scraper de producción (el resto del
# pipeline no procesa casas)
TIPOS_PROPIEDAD_PRODUCCION = ["departamento"]
OPERACION = "arriendo"

MAX_PAGINAS_POR_BUSQUEDA = 1000
MAX_PAGINAS_VACIAS_CONSECUTIVAS = 20
MAX_PAGINAS_POR_CORRIDA = 200
MAX_MINUTOS_POR_CORRIDA = 30
RESULTADOS_POR_PAGINA = 48
DELAY_MIN = 3.0
DELAY_MAX = 7.0

BASE_URL = "https://www.portalinmobiliario.com"

RE_ID_AVISO = re.compile(r"(MLC-\d+)")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}

SELECTORES = {
    "tarjeta": ["div.ui-search-result__wrapper", "div.andes-card", "li.ui-search-layout__item"],
    "titulo": ["h2.ui-search-item__title", "h3.poly-component__title", "a.poly-component__title"],
    "link": ["a.ui-search-link", "a.poly-component__title"],
    "precio": ["span.andes-money-amount__fraction"],
    "moneda": ["span.andes-money-amount__currency-symbol"],
    "ubicacion": ["span.ui-search-item__location", "span.poly-component__location"],
}

RE_DORMITORIOS = re.compile(r"(\d+)\s*dormitorios?", re.IGNORECASE)
RE_BANOS = re.compile(r"(\d+)\s*ba[ñn]os?", re.IGNORECASE)
RE_M2 = re.compile(r"([\d.,]+)\s*m[²2]\b", re.IGNORECASE)


@dataclass
class Aviso:
    comuna: str
    tipo_propiedad: str
    operacion: str
    id_aviso: Optional[str] = None
    titulo: Optional[str] = None
    precio: Optional[str] = None
    moneda: Optional[str] = None
    ubicacion: Optional[str] = None
    dormitorios: Optional[str] = None
    banos: Optional[str] = None
    superficie_m2: Optional[str] = None
    url: Optional[str] = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Crear el esquema de Bronce y la tabla `avisos` (si no existen)
# MAGIC Para que el pipeline se pueda reconstruir desde cero sin pasos manuales: si
# MAGIC el catálogo está vacío, este bloque deja creado el esquema `01_bronce`, la
# MAGIC tabla `avisos` (partición por `first_seen`, fecha de ingesta, nunca se
# MAGIC actualiza después de insertada, `estado_publicacion` vive en
# MAGIC `avisos_detalle`, ver `02_scraper_manual_detalle_bronce`) y la tabla
# MAGIC `control` (clave/valor genérica para estado interno de los scrapers). Si ya
# MAGIC existen, `IF NOT EXISTS` no hace nada.
# MAGIC
# MAGIC `_sistema_origen` y `_id_corrida` son metadata de trazabilidad: de qué
# MAGIC fuente vino la fila y qué ejecución del notebook la escribió (todas las
# MAGIC filas de una misma corrida comparten el mismo `_id_corrida`).

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gran_concepcion.01_bronce")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.avisos (
        id_aviso                     STRING NOT NULL,
        comuna                       STRING,
        tipo_propiedad                STRING,
        operacion                    STRING,
        titulo                       STRING,
        precio                       STRING,
        moneda                       STRING,
        ubicacion                    STRING,
        dormitorios                  STRING,
        banos                        STRING,
        superficie_m2                STRING,
        url                          STRING,
        first_seen                   STRING,
        _sistema_origen               STRING,
        _id_corrida                   STRING
    )
    PARTITIONED BY (first_seen)
""")

# Tabla clave/valor genérica para estado interno de los scrapers (ej. cooldown
# tras CAPTCHA del scraper de detalle, ver 02_scraper_manual_detalle_bronce).
# Es infraestructura operativa del propio pipeline, no una entidad de negocio
# versionada, queda fuera de la discusión de inmutabilidad de Bronce. Se
# crea acá porque este notebook corre primero en la secuencia manual.
spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.control (
        clave STRING NOT NULL,
        valor STRING
    )
""")

print("Esquema y tablas de Bronce verificados/creados.")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 4. Funciones de scraping y parsing
# MAGIC Construcción de URLs de búsqueda, descarga de HTML, y extracción de los
# MAGIC campos de cada tarjeta (título, precio, ubicación, dormitorios, baños,
# MAGIC superficie) desde el HTML de una página de resultados.

# COMMAND ----------

def extraer_id_aviso(url):
    if not url:
        return None
    m = RE_ID_AVISO.search(url)
    return m.group(1) if m else None


def _first_match(soup_or_tag, selector_list):
    for sel in selector_list:
        found = soup_or_tag.select(sel)
        if found:
            return found
    return []


def construir_url(tipo_propiedad, comuna, pagina):
    offset = 1 + (pagina - 1) * RESULTADOS_POR_PAGINA
    if pagina == 1:
        return f"{BASE_URL}/{OPERACION}/{tipo_propiedad}/{comuna}"
    return f"{BASE_URL}/{OPERACION}/{tipo_propiedad}/{comuna}/_Desde_{offset}_NoIndex_True"


def obtener_html(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
    except requests.RequestException as e:
        log.warning(f"Error de red en {url}: {e}")
        return None

    if resp.status_code != 200:
        log.warning(f"Status {resp.status_code} en {url}")
        return None

    if "captcha" in resp.text.lower()[:5000]:
        log.warning(f"Posible CAPTCHA detectado en {url}. Deteniendo esta búsqueda.")
        return None

    return resp.text


def extraer_atributo_texto(tag, selector_list):
    encontrados = _first_match(tag, selector_list)
    if encontrados:
        return encontrados[0].get_text(strip=True)
    return None


def parsear_atributos_regex(tarjeta):
    texto_completo = tarjeta.get_text(" ", strip=True)
    resultado = {"dormitorios": None, "banos": None, "superficie_m2": None}

    m = RE_DORMITORIOS.search(texto_completo)
    if m:
        resultado["dormitorios"] = m.group(1)
    m = RE_BANOS.search(texto_completo)
    if m:
        resultado["banos"] = m.group(1)
    m = RE_M2.search(texto_completo)
    if m:
        resultado["superficie_m2"] = m.group(1)

    return resultado


def parsear_pagina(html, comuna, tipo_propiedad):
    soup = BeautifulSoup(html, "lxml")
    tarjetas = _first_match(soup, SELECTORES["tarjeta"])

    if not tarjetas:
        log.info(f"Sin tarjetas encontradas ({comuna}, {tipo_propiedad}).")
        return []

    avisos = []
    for tarjeta in tarjetas:
        titulo = extraer_atributo_texto(tarjeta, SELECTORES["titulo"])
        precio = extraer_atributo_texto(tarjeta, SELECTORES["precio"])
        moneda = extraer_atributo_texto(tarjeta, SELECTORES["moneda"])
        ubicacion = extraer_atributo_texto(tarjeta, SELECTORES["ubicacion"])

        link_tag = _first_match(tarjeta, SELECTORES["link"])
        url = link_tag[0].get("href") if link_tag else None
        atributos = parsear_atributos_regex(tarjeta)

        avisos.append(Aviso(
            comuna=comuna,
            tipo_propiedad=tipo_propiedad,
            operacion=OPERACION,
            id_aviso=extraer_id_aviso(url),
            titulo=titulo,
            precio=precio,
            moneda=moneda,
            ubicacion=ubicacion,
            dormitorios=atributos["dormitorios"],
            banos=atributos["banos"],
            superficie_m2=atributos["superficie_m2"],
            url=url,
        ))
    return avisos

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 5. Cargar IDs de avisos ya existentes
# MAGIC Trae los IDs ya guardados en Bronce, para no volver a insertar avisos que
# MAGIC ya se scrapearon en una corrida anterior.

# COMMAND ----------

ids_conocidos = set(
    row["id_aviso"] for row in
    spark.sql("SELECT id_aviso FROM gran_concepcion.01_bronce.avisos").collect()
)
log.info(f"{len(ids_conocidos)} avisos ya existen en Bronce.")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 6. Scraping de la grilla, en memoria
# MAGIC Recorre cada combinación de comuna y tipo de propiedad, página por página,
# MAGIC guardando en memoria solo los avisos cuyo ID todavía no existe. Corta por
# MAGIC páginas vacías consecutivas, por techo de páginas totales, o por techo de
# MAGIC tiempo, lo que ocurra primero.

# COMMAND ----------

avisos_nuevos_dicts = []
total_vistos = 0
total_nuevos = 0
paginas_recorridas = 0
motivo_corte = None
t0 = time.time()
hoy = date.today().isoformat()

for comuna in COMUNAS_GRAN_CONCEPCION:
    if motivo_corte in ("limite_paginas", "limite_tiempo"):
        break

    for tipo in TIPOS_PROPIEDAD_PRODUCCION:
        if motivo_corte in ("limite_paginas", "limite_tiempo"):
            break

        log.info(f"--- Buscando: {OPERACION} de {tipo} en {comuna} ---")
        paginas_vacias_consecutivas = 0
        pagina = 1

        while True:
            if paginas_recorridas >= MAX_PAGINAS_POR_CORRIDA:
                motivo_corte = "limite_paginas"
                break
            if (time.time() - t0) / 60 >= MAX_MINUTOS_POR_CORRIDA:
                motivo_corte = "limite_tiempo"
                break
            if paginas_vacias_consecutivas >= MAX_PAGINAS_VACIAS_CONSECUTIVAS:
                break
            if pagina > MAX_PAGINAS_POR_BUSQUEDA:
                break

            url = construir_url(tipo, comuna, pagina)
            log.info(f"Página {pagina}: {url}")
            html = obtener_html(url)
            paginas_recorridas += 1

            if html is None:
                break

            avisos = parsear_pagina(html, comuna, tipo)
            if not avisos:
                break

            nuevos_en_pagina = 0
            for aviso in avisos:
                total_vistos += 1
                if not aviso.id_aviso or aviso.id_aviso in ids_conocidos:
                    continue

                d = asdict(aviso)
                d["first_seen"] = hoy
                d["_sistema_origen"] = SISTEMA_ORIGEN
                d["_id_corrida"] = ID_CORRIDA

                avisos_nuevos_dicts.append(d)
                ids_conocidos.add(aviso.id_aviso)
                nuevos_en_pagina += 1
                total_nuevos += 1

            paginas_vacias_consecutivas = 0 if nuevos_en_pagina > 0 else paginas_vacias_consecutivas + 1
            log.info(f"  -> {len(avisos)} vistos, {nuevos_en_pagina} nuevos "
                      f"(vacías: {paginas_vacias_consecutivas}/{MAX_PAGINAS_VACIAS_CONSECUTIVAS})")

            pagina += 1
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

if motivo_corte is None:
    motivo_corte = "paginas_vacias_consecutivas"

log.info(
    f"Corrida completa. Vistos: {total_vistos} | Nuevos: {total_nuevos} | "
    f"Páginas: {paginas_recorridas} | Motivo de corte: {motivo_corte} | "
    f"Duración: {round(time.time() - t0, 1)}s"
)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 7. Armar DataFrame con los avisos nuevos
# MAGIC Convierte lo recolectado en memoria a un DataFrame de pandas, listo para
# MAGIC insertar.

# COMMAND ----------

df_nuevos = pd.DataFrame(avisos_nuevos_dicts)
print(f"{len(df_nuevos)} avisos nuevos para insertar")
df_nuevos.head()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 8. Crear vista temporal para el INSERT
# MAGIC Punto único de contacto con Spark: convierte el DataFrame de pandas en una
# MAGIC vista SQL temporal.
# MAGIC
# MAGIC Si no hay avisos nuevos (`df_nuevos` vacío, ej. la grilla ya estaba al
# MAGIC día), `spark.createDataFrame(df_nuevos)` no puede inferir un esquema de
# MAGIC un DataFrame de pandas vacío (`CANNOT_INFER_EMPTY_SCHEMA`). En ese caso se
# MAGIC arma la vista vacía con el esquema real de `avisos` (`WHERE 1=0`) en vez
# MAGIC de inferirlo, para que el `MERGE` de la sección 9 quede como no-op en vez
# MAGIC de romper la corrida.

# COMMAND ----------

if len(df_nuevos) == 0:
    print("No hay avisos nuevos: vista temporal vacía, el MERGE de la sección 9 no insertará nada.")
    spark.sql("SELECT * FROM gran_concepcion.01_bronce.avisos WHERE 1 = 0") \
        .createOrReplaceTempView("avisos_nuevos_tmp")
else:
    spark.createDataFrame(df_nuevos).createOrReplaceTempView("avisos_nuevos_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### 9. MERGE final hacia Bronce
# MAGIC Inserta en `avisos` solo los avisos que todavía no existían (columnas
# MAGIC explícitas + casts, para evitar pérdidas silenciosas de tipo).

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gran_concepcion.01_bronce.avisos AS avisos
# MAGIC USING avisos_nuevos_tmp AS nuevos
# MAGIC ON avisos.id_aviso = nuevos.id_aviso
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     id_aviso, comuna, tipo_propiedad, operacion, titulo, precio,
# MAGIC     moneda, ubicacion, dormitorios, banos, superficie_m2, url,
# MAGIC     first_seen, _sistema_origen, _id_corrida
# MAGIC ) VALUES (
# MAGIC     nuevos.id_aviso, 
# MAGIC     nuevos.comuna, 
# MAGIC     nuevos.tipo_propiedad, 
# MAGIC     nuevos.operacion,
# MAGIC     nuevos.titulo, 
# MAGIC     nuevos.precio, 
# MAGIC     nuevos.moneda, 
# MAGIC     nuevos.ubicacion,
# MAGIC     nuevos.dormitorios, 
# MAGIC     nuevos.banos, 
# MAGIC     nuevos.superficie_m2, 
# MAGIC     nuevos.url,
# MAGIC     nuevos.first_seen, 
# MAGIC     nuevos._sistema_origen,
# MAGIC     nuevos._id_corrida
# MAGIC )