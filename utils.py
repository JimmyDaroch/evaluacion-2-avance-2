# -*- coding: utf-8 -*-
"""
Módulo de Utilidades de ETL (Evaluación de Arquitectura y Almacenamiento de Datos)
--------------------------------------------------------------------------------
Este módulo proporciona funciones avanzadas y altamente resilientes de ETL:
1. Decodificador robusto multi-encoding (UTF-8, Latin-1, CP1252, UTF-16, etc.) con reemplazo de emergencia.
2. Detección dinámica de delimitadores con fallback y soporte para espacios múltiples.
3. Parseo inteligente de fechas mezcladas mediante expresiones regulares y 'dateutil.parser'.
"""

import re
import io
import csv
import logging
from datetime import datetime
from dateutil import parser

# Configurar carpeta de logs persistente
import os
from logging.handlers import RotatingFileHandler

LOGS_DIR = 'logs'
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOGS_DIR, 'etl.log')

# Crear formateador común
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Configurar logger raíz
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Limpiar manejadores para evitar duplicados en reinicios de Flask
if root_logger.hasHandlers():
    root_logger.handlers.clear()

# Manejador de consola
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
root_logger.addHandler(console_handler)

# Manejador rotativo (2MB por archivo, guarda 5 respaldos)
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2*1024*1024, backupCount=5, encoding='utf-8')
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)

def decode_content(content_bytes):
    """
    Intenta decodificar secuencialmente un conjunto de bytes probando distintos formatos
    comunes (UTF-8, Latin-1, CP1252, UTF-16, UTF-16-LE, UTF-16-BE) para evitar caídas catastróficas.
    Retorna una tupla: (texto_decodificado, encoding_detectado).
    """
    if not content_bytes:
        return "", "VACÍO"
        
    # Verificar indicios fuertes de UTF-16 (BOM o presencia de bytes nulos)
    has_utf16_bom = content_bytes.startswith(b'\xff\xfe') or content_bytes.startswith(b'\xfe\xff')
    has_null_bytes = b'\x00' in content_bytes
    
    if has_utf16_bom or has_null_bytes:
        # Priorizar UTF-16 si hay fuertes sospechas (BOM o bytes nulos)
        encodings = ['utf-16', 'utf-16-le', 'utf-16-be', 'utf-8', 'latin-1', 'cp1252']
    else:
        # Priorizar UTF-8 y Latin-1 si no hay sospechas de UTF-16 (evita falsos de codificación accidental)
        encodings = ['utf-8', 'latin-1', 'cp1252', 'utf-16', 'utf-16-le', 'utf-16-be']
    
    for enc in encodings:
        try:
            # Si el encoding es UTF-16 y no hay un BOM, a veces puede fallar de forma silenciosa o ruidosa.
            # Este try lo maneja.
            decoded = content_bytes.decode(enc)
            logging.info(f"Decodificación exitosa con codificación: {enc.upper()}")
            return decoded, enc.upper()
        except (UnicodeDecodeError, LookupError):
            continue
            
    # Fallback extremo: decodificar con reemplazo de caracteres rotos
    logging.warning("Fallo en todas las codificaciones estándar. Aplicando decodificación fallback con reemplazo.")
    return content_bytes.decode('utf-8', errors='replace'), 'UTF-8 (REEMPLAZO/FALLBACK)'

def detect_delimiter(file_content, default=','):
    """
    Detecta dinámicamente el delimitador más probable en un archivo analizando la consistencia.
    Soporta: , ; \t | y múltiples espacios en blanco ('SPACES').
    Si no se encuentra un delimitador obvio, revisa alternativas para evitar caídas.
    """
    if not file_content:
        return default
        
    # Tomamos las primeras 15 líneas no vacías para el análisis estructural
    lines = [line.strip() for line in file_content.split('\n') if line.strip()][:15]
    if not lines:
        return default
        
    candidates = [',', ';', '\t', '|']
    counts = {c: [] for c in candidates}
    
    for line in lines:
        for c in candidates:
            counts[c].append(line.count(c))
            
    best_delim = default
    max_consistent_count = -1
    
    for c, freq in counts.items():
        avg = sum(freq) / len(freq)
        if avg > 0:
            # Varianza para medir regularidad estructural por línea
            variance = sum((x - avg) ** 2 for x in freq) / len(freq)
            # Priorizamos delimitadores muy consistentes por fila
            if avg > max_consistent_count and variance < 0.5:
                max_consistent_count = avg
                best_delim = c
                
    # Fallback si no hay delimitador estándar consistente
    if max_consistent_count <= 0:
        total_counts = {c: sum(counts[c]) for c in candidates}
        best_delim = max(total_counts, key=total_counts.get)
        
        # Si de verdad no hay ningún delimitador estándar, ver si es por espacios múltiples (formato alineado por columnas)
        if total_counts[best_delim] == 0:
            # Contar divisiones de 2 o más espacios consecutivos en cada línea
            space_splits = [len(re.split(r'\s{2,}', line)) - 1 for line in lines]
            avg_spaces = sum(space_splits) / len(space_splits)
            if avg_spaces > 0:
                logging.info("Delimitador detectado: Múltiples Espacios (SPACES)")
                return 'SPACES'
                
            # Por último, si todo falla, devolver el delimitador por defecto
            best_delim = default
            
    logging.info(f"Delimitador detectado: '{best_delim}'")
    return best_delim

def split_line_by_delimiter(line, delimiter):
    """
    Divide una línea de texto basada en el delimitador detectado.
    Soporta delimitación estándar y la marca especial 'SPACES'.
    Es altamente tolerante a errores estructurales.
    """
    if not line:
        return []
        
    # Caso 1: Delimitado por múltiples espacios
    if delimiter == 'SPACES':
        return [f.strip() for f in re.split(r'\s{2,}', line.strip()) if f.strip() != '']
        
    # Caso 2: Tabulador simple
    if delimiter == '\t':
        return [f.strip() for f in line.split('\t')]
        
    # Caso 3: Delimitación estándar con comillas opcionales
    # Usamos el parser nativo de CSV por línea para soportar textos entrecomillados que contengan el separador
    f_in = io.StringIO(line)
    reader = csv.reader(f_in, delimiter=delimiter)
    try:
        rows = list(reader)
        if rows:
            return [f.strip() for f in rows[0]]
        return []
    except Exception:
        # Fallback si la librería csv falla por comillas corruptas (ej. Pedro "El Pascal)
        return [f.strip() for f in line.split(delimiter)]

def clean_text_field(text):
    r"""
    Remueve caracteres de basura textual explícitos indicados en la pauta académica (@#;:|\/*).
    Además, limpia tildes/acentos y espacios múltiples y en extremos (Fuzzy-prep).
    """
    if text is None:
        return ""
        
    cleaned = str(text)
    # Lista de caracteres prohibidos en campos textuales
    chars_to_remove = ['@', '#', ';', ':', '|', '/', '\\', '*']
    for char in chars_to_remove:
        cleaned = cleaned.replace(char, '')
        
    # Limpieza de acentos y tildes comunes
    replacements = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'ü': 'u', 'ñ': 'n',
        'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
        'Ü': 'U', 'Ñ': 'N'
    }
    for key, val in replacements.items():
        cleaned = cleaned.replace(key, val)
        
    # Eliminar múltiples espacios en blanco consecutivos y recortar extremos (Fuzzy trim)
    cleaned = ' '.join(cleaned.split()).strip()
    return cleaned

def unify_date(date_text):
    """
    Estandariza cualquier formato de fecha a formato chileno 'DD-MM-YYYY' utilizando
    heurísticas de limpieza regex y la biblioteca de parseo difuso 'dateutil.parser'.
    
    Soporta: 1956/07/09, 09-07-1956, 07-09-1956, 1956-07-09, July 9 1956, 9 Jul 1956, 09 Jul 56,
    fechas embebidas en cadenas como "Nacido el 19 de diciembre de 1988 en Santiago".
    """
    if not date_text:
        raise ValueError("Fecha nula o vacía")
        
    text_clean = str(date_text).strip()
    text_lower = text_clean.lower()
    
    # 0. Limpieza extrema: colapsar guiones o barras múltiples (ej: 1956///07///09 -> 1956-07-09)
    # y eliminar espacios múltiples redundantes.
    text_lower = re.sub(r'[-/]+', '-', text_lower)
    text_lower = ' '.join(text_lower.split())
    
    # 1. Traducir meses en español a inglés para compatibilidad total con dateutil
    spanish_months = {
        'enero': 'january', 'febrero': 'february', 'marzo': 'march', 'abril': 'april',
        'mayo': 'may', 'junio': 'june', 'julio': 'july', 'agosto': 'august',
        'septiembre': 'september', 'octubre': 'october', 'noviembre': 'november', 'diciembre': 'december',
        'ene': 'jan', 'feb': 'feb', 'mar': 'mar', 'abr': 'apr',
        'ago': 'aug', 'sep': 'sep', 'oct': 'oct', 'nov': 'nov', 'dic': 'dec'
    }
    for sp_m, en_m in spanish_months.items():
        text_lower = re.sub(r'\b' + re.escape(sp_m) + r'\b', en_m, text_lower)
    
    # 2. Quitar palabras comunes de ruido en español e inglés
    noise_words = [
        'nacido', 'nacida', 'el', 'en', 'born', 'on', 'at', 'fecha', 'de', 
        'nacimiento', ':', ',', 'star', 'famoso', 'celebridad', 'celebrity'
    ]
    for word in noise_words:
        text_lower = re.sub(r'\b' + re.escape(word) + r'\b', '', text_lower)
        
    # Quitar símbolos sobrantes al inicio y final
    text_lower = text_lower.strip(' .*#@;:\\/+-')
    
    # 3. Detectar formatos ISO YYYY-MM-DD o YYYY/MM/DD y parsear directamente para evitar que dayfirst=True los confunda
    iso_match = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', text_lower)
    if iso_match:
        year, month, day = iso_match.groups()
        try:
            dt = datetime(int(year), int(month), int(day))
            return dt.strftime('%d-%m-%Y')
        except ValueError:
            pass
            
    # 4. Intentar parseo directo con dateutil
    # 'dayfirst=True' es vital para desempatar formatos ambiguos (ej: 09-07-1956 -> 9 de Julio, no 7 de Septiembre)
    try:
        dt = parser.parse(text_lower, fuzzy=True, dayfirst=True)
        return dt.strftime('%d-%m-%Y')
    except Exception:
        pass
        
    # 5. Si falla, aplicar pre-extracción regex de segmentos probables de fechas
    # Patrón 1: fechas con nombres de meses textuales (ej. "July 9 1956", "9 jul 1956")
    # Patrón 2: formato numérico estándar (ej. "1956-07-09" o "09/07/1956")
    patrones = [
        r'(\d{1,2})\s*[-/]?\s*([a-zA-Z]{3,10})\s*[-/]?\s*(\d{2,4})', # DD-Mes-YYYY
        r'([a-zA-Z]{3,10})\s*[-/]?\s*(\d{1,2})\s*[-/]?\s*(\d{2,4})', # Mes-DD-YYYY
        r'(\d{2,4})[-/](\d{1,2})[-/](\d{2,4})'                       # Numérico
    ]
    
    for patron in patrones:
        match = re.search(patron, text_lower)
        if match:
            candidate = match.group(0)
            try:
                # Comprobar si también es un patrón ISO en la regex
                iso_cand = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', candidate)
                if iso_cand:
                    year, month, day = iso_cand.groups()
                    dt = datetime(int(year), int(month), int(day))
                else:
                    dt = parser.parse(candidate, fuzzy=True, dayfirst=True)
                return dt.strftime('%d-%m-%Y')
            except Exception:
                continue
                
    raise ValueError(f"Formato de fecha irreconocible por dateutil y regex: '{date_text}'")
