# -*- coding: utf-8 -*-
r"""
Módulo ETL Famosos y Fechas - Parte 1
--------------------------------------------------------------------------------
Este módulo implementa el pipeline ETL completo para el dataset de Famosos:
1. Lectura automática y detección del delimitador con fallback.
2. Limpieza de caracteres textuales basura (@#;:|\/*).
3. Normalización y unificación de fechas a formato chileno (DD-MM-YYYY) usando dateutil.
4. Detección inteligente de duplicados mediante Normalización Fuzzy (trim, lowercase, tildes, etc.)
5. Cálculo dinámico de edad y activación de cumpleaños_flag.
6. Guardado relacional en base de datos sin crashes.
"""

import logging
from datetime import datetime
from rapidfuzz import fuzz

from utils import detect_delimiter, clean_text_field, unify_date, split_line_by_delimiter
from database import save_famosos

def calculate_age_and_flag(birth_date_str):
    """
    Calcula la edad de forma dinámica respecto al año actual y determina
    si el cumpleaños de la persona es el día de hoy (cumpleanos_flag).
    """
    try:
        birth_dt = datetime.strptime(birth_date_str, "%d-%m-%Y")
    except ValueError:
        return 0, False
        
    today = datetime.now()
    age = today.year - birth_dt.year
    if (today.month, today.day) < (birth_dt.month, birth_dt.day):
        age -= 1
        
    cumpleanos_flag = (today.month == birth_dt.month and today.day == birth_dt.day)
    return age, cumpleanos_flag

def normalize_for_fuzzy_match(text):
    """
    Normaliza exhaustivamente cadenas de texto para comparaciones de deduplicación fuzzy.
    Realiza: lowercase, trim, remoción de espacios múltiples, conversión de acentos y
    reemplazo de guiones y guiones bajos por espacios.
    """
    if not text:
        return ""
        
    # Reemplazar guiones y guiones bajos por espacios para manejar "Tom-Hanks" y "Tom_Hanks"
    normalized = str(text).lower().strip().replace('-', ' ').replace('_', ' ')
    
    # Remplazar vocales con tildes comunes
    replacements = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'ü': 'u', 'ñ': 'n'
    }
    for key, val in replacements.items():
        normalized = normalized.replace(key, val)
        
    # Colapsar múltiples espacios consecutivos
    normalized = ' '.join(normalized.split())
    return normalized

def are_names_similar(name1, name2, threshold=85.0):
    """
    Compara la similitud de dos nombres mediante la normalización fuzzy base
    y una comparación probabilística de Levenshtein usando RapidFuzz.
    """
    n1 = normalize_for_fuzzy_match(name1)
    n2 = normalize_for_fuzzy_match(name2)
    
    if not n1 or not n2:
        return False
        
    # Coincidencia exacta post-normalización
    if n1 == n2:
        return True
        
    ratio = fuzz.ratio(n1, n2)
    return ratio >= threshold

def process_famosos_etl(file_content, mapping=None):
    """
    Ejecuta el pipeline ETL para el dataset de Famosos.
    Soporta alias ampliados de columnas y es inmune a crashes estructurales.
    """
    # =========================================================================
    # 📥 [ETL: EXTRACCIÓN] - Lectura del Dataset y Auto-detección Estructural
    # =========================================================================
    # 1. Detectar delimitador de forma dinámica analizando varianza por fila
    delimiter = detect_delimiter(file_content)
    
    # 2. Separar líneas no vacías para su lectura
    raw_lines = [line.strip() for line in file_content.split('\n') if line.strip()]
    if not raw_lines:
        logging.warning("[ETL FAMOSOS] Intento de procesar archivo vacío o sin líneas válidas.")
        return {
            'success': False,
            'message': 'El archivo está vacío.',
            'original_count': 0,
            'cleaned_count': 0
        }
        
    # 3. Analizar si la primera línea contiene cabeceras e identificar las columnas
    first_line_fields = split_line_by_delimiter(raw_lines[0], delimiter)
    has_headers = False
    
    headers_cleaned = [clean_text_field(h).lower() for h in first_line_fields]
    
    # Listas exhaustivas de alias dinámicos (Sinónimos académicos)
    alias_nombre = ['nombre', 'nombres', 'persona', 'celebrity', 'famoso', 'full_name', 'character', 'name', 'celebridad']
    alias_fecha = ['fecha', 'birthdate', 'dob', 'birthday', 'fecha_nacimiento', 'nacimiento', 'date', 'nacido']
    
    # Heurística para saber si la primera fila contiene metadatos de cabecera
    if any(any(k in h for k in (alias_nombre + alias_fecha)) for h in headers_cleaned):
        has_headers = True
        
    name_idx = 0
    date_idx = 1
    
    # Asignación de índices por mapeo del usuario (UI)
    if mapping:
        try:
            name_idx = int(mapping.get('nombre_idx', 0))
            date_idx = int(mapping.get('fecha_idx', 1))
            data_lines = raw_lines[1:] if has_headers else raw_lines
            logging.info(f"[ETL FAMOSOS] Mapeo explícito proporcionado - Nombre Col index: {name_idx}, Fecha Col index: {date_idx}")
        except (ValueError, IndexError) as e:
            logging.warning(f"[ETL FAMOSOS] Mapeo de columnas inválido, usando heurística. Error: {str(e)}")
            data_lines = raw_lines
    else:
        # Asignación por auto-detección heurística flexible con alias ampliados
        if has_headers:
            assigned_name = False
            assigned_date = False
            for idx, h in enumerate(headers_cleaned):
                if any(k == h or k in h for k in alias_nombre) and not assigned_name:
                    name_idx = idx
                    assigned_name = True
                elif any(k == h or k in h for k in alias_fecha) and not assigned_date:
                    date_idx = idx
                    assigned_date = True
            
            # Si no se detectó alguna columna, forzar descarte posicional
            if not assigned_name: name_idx = 0
            if not assigned_date: date_idx = 1 if len(first_line_fields) > 1 else 0
            
            data_lines = raw_lines[1:]
            logging.info(f"[ETL FAMOSOS] Auto-detección de columnas completada - Nombre index: {name_idx} ('{first_line_fields[name_idx]}'), Fecha index: {date_idx} ('{first_line_fields[date_idx]}')")
        else:
            # Si no hay cabeceras, asumimos Nombre = col 0, Fecha = col 1
            data_lines = raw_lines
            logging.info("[ETL FAMOSOS] Sin cabeceras detectadas. Usando mapeo por defecto (Col 0: Nombre, Col 1: Fecha)")

    original_count = len(data_lines)
    logging.info(f"[ETL FAMOSOS] Iniciando procesamiento de Famosos. Total filas a procesar: {original_count}. Delimitador: '{delimiter}'")
    
    clean_records = []
    corrupt_records = []
    duplicate_records = []
    
    # Iterar y procesar cada registro extraído
    for idx, line in enumerate(data_lines):
        line_num = idx + (2 if has_headers else 1)
        
        # Parsear la línea con el delimitador correspondiente
        row = split_line_by_delimiter(line, delimiter)
        
        # =====================================================================
        # ⚙️ [ETL: TRANSFORMACIÓN] - Limpieza, Normalización y Deduplicación Fuzzy
        # =====================================================================
        
        # A. Validación de columnas suficientes (Evita caídas por filas incompletas)
        if len(row) <= max(name_idx, date_idx):
            reason = f'Estructura incompleta. Fila contiene {len(row)} columnas, pero requiere índice {max(name_idx, date_idx)+1}'
            logging.warning(f"[ETL FAMOSOS] Fila {line_num} excluida - {reason}. Fila original: '{line}'")
            corrupt_records.append({
                'line': line_num,
                'raw': line,
                'reason': reason
            })
            continue
            
        raw_name = row[name_idx]
        raw_date = row[date_idx]
        
        # B. Limpieza de Nombre (Eliminación de caracteres prohibidos @#;:|/* y espacios extra)
        clean_name = clean_text_field(raw_name)
        if not clean_name:
            reason = 'Nombre del famoso nulo o vacío tras la limpieza de caracteres prohibidos'
            logging.warning(f"[ETL FAMOSOS] Fila {line_num} excluida - {reason}. Fila original: '{line}'")
            corrupt_records.append({
                'line': line_num,
                'raw': line,
                'reason': reason
            })
            continue
            
        # C. Normalización y unificación de fechas a formato chileno (DD-MM-YYYY) usando dateutil difuso
        try:
            clean_date = unify_date(raw_date)
        except Exception as e:
            reason = f'Fecha inválida o incomprensible ("{raw_date}"). Error: {str(e)}'
            logging.warning(f"[ETL FAMOSOS] Fila {line_num} excluida - {reason}. Fila original: '{line}'")
            corrupt_records.append({
                'line': line_num,
                'raw': line,
                'reason': reason
            })
            continue
            
        # D. Cálculo automático de Edad y Flag de Cumpleaños respecto a la fecha actual
        edad, cumple_flag = calculate_age_and_flag(clean_date)
        
        record = {
            'nombre': clean_name,
            'fecha_nacimiento': clean_date,
            'edad': edad,
            'cumpleanos_flag': cumple_flag,
            'raw_row': line,
            'line': line_num
        }
        
        # E. Deduplicación lógica inteligente usando RapidFuzz (Algoritmo Levenshtein >= 85%)
        is_duplicate = False
        duplicate_reason = ""
        matching_kept_record = None
        
        for kept in clean_records:
            # Caso 1: Comparación exacta del nombre fuzzy (ignorando case, espacios, tildes, guiones) y misma fecha de nacimiento
            if normalize_for_fuzzy_match(kept['nombre']) == normalize_for_fuzzy_match(clean_name) and kept['fecha_nacimiento'] == clean_date:
                is_duplicate = True
                duplicate_reason = "Duplicidad lógica exacta (Nombres idénticos tras fuzzy trim y misma fecha de nacimiento)"
                matching_kept_record = kept
                break
            # Caso 2: Coincidencia probabilística alta (>85%) y misma fecha
            elif kept['fecha_nacimiento'] == clean_date and are_names_similar(kept['nombre'], clean_name):
                is_duplicate = True
                duplicate_reason = f"Deduplicación Fuzzy Parcial con '{kept['nombre']}' (Nombres altamente equivalentes con la misma fecha)"
                matching_kept_record = kept
                break
                
        if is_duplicate:
            logging.warning(f"[ETL FAMOSOS] Fila {line_num} excluida - {duplicate_reason}. Fila original: '{line}'")
            duplicate_records.append({
                'original': {
                    'line': line_num,
                    'nombre': clean_name,
                    'fecha_nacimiento': clean_date,
                    'raw': line
                },
                'kept': {
                    'line': matching_kept_record['line'],
                    'nombre': matching_kept_record['nombre'],
                    'fecha_nacimiento': matching_kept_record['fecha_nacimiento']
                },
                'reason': duplicate_reason
            })
        else:
            clean_records.append(record)
            
    # =========================================================================
    # 💾 [ETL: CARGA] - Persistencia Relacional en Base de Datos SQLite
    # =========================================================================
    try:
        save_famosos(clean_records)
        logging.info(f"[ETL FAMOSOS] Datos persistidos exitosamente en SQLite. Registros guardados: {len(clean_records)}")
    except Exception as e:
        logging.error(f"[ETL FAMOSOS] Falla de persistencia en SQLite: {str(e)}")
        return {
            'success': False,
            'message': f'Falla de persistencia en SQLite: {str(e)}',
            'original_count': original_count,
            'cleaned_count': 0
        }
        
    logging.info(f"[ETL FAMOSOS] ETL completado. Originales: {original_count}, Guardados: {len(clean_records)}, Corruptos: {len(corrupt_records)}, Duplicados: {len(duplicate_records)}")
    return {
        'success': True,
        'delimiter_detected': delimiter,
        'has_headers': has_headers,
        'mapped_columns': {
            'nombre_col': first_line_fields[name_idx] if has_headers and name_idx < len(first_line_fields) else f"Columna {name_idx}",
            'fecha_col': first_line_fields[date_idx] if has_headers and date_idx < len(first_line_fields) else f"Columna {date_idx}"
        },
        'original_count': original_count,
        'cleaned_count': len(clean_records),
        'corrupt_count': len(corrupt_records),
        'duplicate_count': len(duplicate_records),
        'clean_records': clean_records,
        'corrupt': corrupt_records,
        'duplicates': duplicate_records
    }
