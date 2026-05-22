# -*- coding: utf-8 -*-
"""
Módulo ETL Normalización de Lugares - Parte 2
--------------------------------------------------------------------------------
Este módulo implementa el pipeline ETL para el dataset de Lugares.
Se encarga de:
1. Leer el archivo y detectar automáticamente cabeceras y delimitadores con fallback.
2. Limpiar caracteres textuales basura de todos los campos.
3. Mapear de forma altamente flexible columnas usando un amplio rango de alias y sinónimos.
4. Dividir la información en tres entidades lógicas normalizadas en 3FN:
   - Lugares
   - Georeferencias (latitud, longitud con validación estricta de cotas físicas)
   - Direcciones (calle, número, ciudad/provincia, país)
5. Resolver llaves foráneas y guardar relacionalmente en SQLite.
"""

import logging
from utils import detect_delimiter, clean_text_field, split_line_by_delimiter
from database import save_lugares_relacional

def guess_column_indices(first_row, headers_detected):
    """
    Mapea de forma heurística y flexible los índices de las columnas analizando
    los sinónimos y alias más comunes (soporta alias como coord_x, place, etc.).
    """
    indices = {
        'lugar_idx': 0,
        'lat_idx': None,
        'lon_idx': None,
        'calle_idx': None,
        'numero_idx': None,
        'ciudad_idx': None,
        'pais_idx': None
    }
    
    # Listas amplias de sinónimos/alias
    synonyms = {
        'lugar_idx': ['lugar', 'place', 'sitio', 'nombre', 'name', 'establecimiento', 'location', 'lugar_nombre', 'nombre_lugar'],
        'lat_idx': ['lat', 'latitude', 'latitud', 'coord_x', 'coordenada_x', 'coordenada_y'],
        'lon_idx': ['lon', 'lng', 'longitude', 'longitud', 'coord_y', 'coordenada_y', 'coordenada_x'],
        'calle_idx': ['calle', 'street', 'direccion', 'address', 'via'],
        'numero_idx': ['numero', 'num', 'number', 'nro', 'altura'],
        'ciudad_idx': ['ciudad', 'city', 'provincia', 'estado', 'region', 'comuna', 'provincia_estado'],
        'pais_idx': ['pais', 'country', 'nacion']
    }
    
    assigned = set()
    
    # 1. Intentar asignación directa por palabra clave
    for key, words in synonyms.items():
        for idx, h in enumerate(headers_detected):
            if any(w == h or w in h for w in words) and idx not in assigned:
                indices[key] = idx
                assigned.add(idx)
                break
                
    # 2. Heurística especial de desambiguación para coordenadas lat/lon si se usó coord_x/y
    # Si lat y lon se cruzaron, desempatamos
    if indices['lat_idx'] is not None and indices['lon_idx'] is not None:
        lat_lbl = headers_detected[indices['lat_idx']]
        lon_lbl = headers_detected[indices['lon_idx']]
        # Si la columna latitud tiene 'y' o 'lon' y la otra tiene 'x' o 'lat', las intercambiamos
        if ('y' in lat_lbl or 'lon' in lat_lbl or 'lng' in lat_lbl) and ('x' in lon_lbl or 'lat' in lon_lbl):
            # Intercambiar
            indices['lat_idx'], indices['lon_idx'] = indices['lon_idx'], indices['lat_idx']

    # 3. Asignaciones por descarte por orden físico si quedaron nulos
    if indices['lugar_idx'] is None and len(first_row) > 0:
        indices['lugar_idx'] = 0
        assigned.add(0)
        
    defaults = ['lugar_idx', 'lat_idx', 'lon_idx', 'calle_idx', 'numero_idx', 'ciudad_idx', 'pais_idx']
    for idx, key in enumerate(defaults):
        if indices[key] is None and idx < len(first_row) and idx not in assigned:
            indices[key] = idx
            assigned.add(idx)
            
    return indices

def process_lugares_etl(file_content, mapping=None):
    """
    Ejecuta el pipeline ETL para el dataset de Lugares.
    Asegura tolerancia máxima y descomposición en 3FN sin crashes.
    """
    # =========================================================================
    # 📥 [ETL: EXTRACCIÓN] - Lectura del Dataset y Mapeo Flexible de Columnas
    # =========================================================================
    # 1. Detectar delimitador de forma dinámica analizando regularidad
    delimiter = detect_delimiter(file_content)
    
    # 2. Separar líneas del archivo para procesar
    raw_lines = [line.strip() for line in file_content.split('\n') if line.strip()]
    if not raw_lines:
        logging.warning("[ETL LUGARES] Intento de procesar archivo vacío o sin líneas válidas.")
        return {
            'success': False,
            'message': 'El archivo está vacío.',
            'original_count': 0,
            'cleaned_count': 0
        }
        
    # 3. Analizar cabeceras e identificar columnas
    first_line_fields = split_line_by_delimiter(raw_lines[0], delimiter)
    has_headers = False
    
    headers_cleaned = [clean_text_field(h).lower() for h in first_line_fields]
    
    # Palabras clave ampliadas para detectar si la primera línea contiene cabeceras
    header_keywords = ['lugar', 'place', 'nombre', 'name', 'lat', 'lon', 'lng', 'calle', 'street', 'direccion', 'ciudad', 'pais', 'coord_x', 'coord_y']
    if any(any(k in h for k in header_keywords) for h in headers_cleaned):
        has_headers = True
        
    # Resolver índices de columnas dinámicamente
    if mapping:
        indices = {
            'lugar_idx': int(mapping.get('lugar_idx', 0)) if mapping.get('lugar_idx') != '' else 0,
            'lat_idx': int(mapping.get('lat_idx')) if mapping.get('lat_idx') not in [None, ''] else None,
            'lon_idx': int(mapping.get('lon_idx')) if mapping.get('lon_idx') not in [None, ''] else None,
            'calle_idx': int(mapping.get('calle_idx')) if mapping.get('calle_idx') not in [None, ''] else None,
            'numero_idx': int(mapping.get('numero_idx')) if mapping.get('numero_idx') not in [None, ''] else None,
            'ciudad_idx': int(mapping.get('ciudad_idx')) if mapping.get('ciudad_idx') not in [None, ''] else None,
            'pais_idx': int(mapping.get('pais_idx')) if mapping.get('pais_idx') not in [None, ''] else None
        }
        data_lines = raw_lines[1:] if has_headers else raw_lines
        logging.info(f"[ETL LUGARES] Mapeo explícito de columnas: {indices}")
    else:
        # Auto-detección flexible con alias ampliados
        if has_headers:
            indices = guess_column_indices(first_line_fields, headers_cleaned)
            data_lines = raw_lines[1:]
            logging.info(f"[ETL LUGARES] Auto-detección de columnas completada: {indices}")
        else:
            # Layout posicional fijo por defecto
            indices = {
                'lugar_idx': 0,
                'lat_idx': 1 if len(first_line_fields) > 1 else None,
                'lon_idx': 2 if len(first_line_fields) > 2 else None,
                'calle_idx': 3 if len(first_line_fields) > 3 else None,
                'numero_idx': 4 if len(first_line_fields) > 4 else None,
                'ciudad_idx': 5 if len(first_line_fields) > 5 else None,
                'pais_idx': 6 if len(first_line_fields) > 6 else None
            }
            data_lines = raw_lines
            logging.info("[ETL LUGARES] Sin cabeceras detectadas. Usando mapeo posicional fijo.")

    original_count = len(data_lines)
    logging.info(f"[ETL LUGARES] Iniciando procesamiento de Lugares. Total filas a procesar: {original_count}. Delimitador: '{delimiter}'")
    
    clean_records = []
    corrupt_records = []
    duplicate_records = []
    
    seen_places = set()
    
    # Iterar y procesar cada registro extraído
    for idx, line in enumerate(data_lines):
        line_num = idx + (2 if has_headers else 1)
        row = split_line_by_delimiter(line, delimiter)
        
        # =====================================================================
        # ⚙️ [ETL: TRANSFORMACIÓN] - Limpieza de Caracteres, Normalización 3FN y Georreferencia
        # =====================================================================
        
        # A. Validación de columna obligatoria de Nombre de Lugar
        lugar_idx = indices['lugar_idx']
        if len(row) <= lugar_idx:
            reason = f'Fila incompleta. Contiene {len(row)} columnas, pero requiere índice {lugar_idx+1} para el Nombre del Lugar'
            logging.warning(f"[ETL LUGARES] Fila {line_num} excluida - {reason}. Fila original: '{line}'")
            corrupt_records.append({
                'line': line_num,
                'raw': line,
                'reason': reason
            })
            continue
            
        raw_lugar = row[lugar_idx]
        # B. Limpieza de Texto del Nombre (Remover @#;:|/* y trim)
        clean_lugar = clean_text_field(raw_lugar)
        
        if not clean_lugar:
            reason = 'Nombre del lugar nulo o vacío tras la limpieza de caracteres textuales basura'
            logging.warning(f"[ETL LUGARES] Fila {line_num} excluida - {reason}. Fila original: '{line}'")
            corrupt_records.append({
                'line': line_num,
                'raw': line,
                'reason': reason
            })
            continue
            
        # C. Conversión y validación de Georreferencias con cotas geográficas físicas estrictas
        lat_val = None
        lon_val = None
        is_coord_error = False
        
        # Validar y Limpiar Latitud (Cotas: -90 a 90)
        if indices['lat_idx'] is not None and indices['lat_idx'] < len(row):
            raw_lat = clean_text_field(row[indices['lat_idx']])
            if raw_lat:
                try:
                    lat_val = float(raw_lat.replace(',', '.'))
                    if not (-90.0 <= lat_val <= 90.0):
                        reason = f'Cota física de latitud excedida (-90 a 90): "{raw_lat}"'
                        logging.warning(f"[GEOGRAFÍA] Fila {line_num} excluida - Cota física de latitud excedida: {raw_lat}")
                        corrupt_records.append({
                            'line': line_num,
                            'raw': line,
                            'reason': reason
                        })
                        is_coord_error = True
                except ValueError:
                    reason = f'Valor de latitud no numérico: "{raw_lat}"'
                    logging.warning(f"[GEOGRAFÍA] Fila {line_num} excluida - Latitud no numérica: {raw_lat}")
                    corrupt_records.append({
                        'line': line_num,
                        'raw': line,
                        'reason': reason
                    })
                    is_coord_error = True
                    
        # Validar y Limpiar Longitud (Cotas: -180 a 180)
        if indices['lon_idx'] is not None and indices['lon_idx'] < len(row) and not is_coord_error:
            raw_lon = clean_text_field(row[indices['lon_idx']])
            if raw_lon:
                try:
                    lon_val = float(raw_lon.replace(',', '.'))
                    if not (-180.0 <= lon_val <= 180.0):
                        reason = f'Cota física de longitud excedida (-180 a 180): "{raw_lon}"'
                        logging.warning(f"[GEOGRAFÍA] Fila {line_num} excluida - Cota física de longitud excedida: {raw_lon}")
                        corrupt_records.append({
                            'line': line_num,
                            'raw': line,
                            'reason': reason
                        })
                        is_coord_error = True
                except ValueError:
                    reason = f'Valor de longitud no numérico: "{raw_lon}"'
                    logging.warning(f"[GEOGRAFÍA] Fila {line_num} excluida - Longitud no numérica: {raw_lon}")
                    corrupt_records.append({
                        'line': line_num,
                        'raw': line,
                        'reason': reason
                    })
                    is_coord_error = True
                    
        if is_coord_error:
            continue
            
        # D. Parseo y limpieza de campos de Dirección para normalización 3FN
        calle_val = "Sin Nombre"
        if indices['calle_idx'] is not None and indices['calle_idx'] < len(row):
            calle_val = clean_text_field(row[indices['calle_idx']]) or "Sin Nombre"
            
        numero_val = ""
        if indices['numero_idx'] is not None and indices['numero_idx'] < len(row):
            numero_val = clean_text_field(row[indices['numero_idx']])
            
        ciudad_val = "Desconocido"
        if indices['ciudad_idx'] is not None and indices['ciudad_idx'] < len(row):
            ciudad_val = clean_text_field(row[indices['ciudad_idx']]) or "Desconocido"
            
        pais_val = "Chile"
        if indices['pais_idx'] is not None and indices['pais_idx'] < len(row):
            pais_val = clean_text_field(row[indices['pais_idx']]) or "Chile"
            
        # E. Deduplicación relacional lógica mediante clave compuesta (Evita redundancia en 3FN)
        lugar_key = (
            clean_lugar.lower().strip(),
            calle_val.lower().strip(),
            str(numero_val).lower().strip(),
            ciudad_val.lower().strip(),
            pais_val.lower().strip()
        )
        
        record = {
            'nombre_lugar': clean_lugar,
            'latitud': lat_val,
            'longitud': lon_val,
            'nombre_calle': calle_val,
            'numero_calle': numero_val,
            'ciudad_estado_provincia': ciudad_val,
            'pais': pais_val,
            'raw_row': line,
            'line': line_num
        }
        
        if lugar_key in seen_places:
            duplicate_reason = 'Duplicidad relacional (Mismo nombre de establecimiento y misma dirección física)'
            logging.warning(f"[ETL LUGARES] Fila {line_num} excluida - {duplicate_reason}. Fila original: '{line}'")
            duplicate_records.append({
                'original': {
                    'line': line_num,
                    'nombre_lugar': clean_lugar,
                    'direccion': f"{calle_val} {numero_val}, {ciudad_val}, {pais_val}",
                    'raw': line
                },
                'reason': duplicate_reason
            })
        else:
            seen_places.add(lugar_key)
            clean_records.append(record)
            
    # =========================================================================
    # 💾 [ETL: CARGA] - Persistencia Relacional y Normalizada (3FN) en SQLite
    # =========================================================================
    try:
        save_lugares_relacional(clean_records)
        logging.info(f"[ETL LUGARES] Inserción relacional completa (3FN) en SQLite. Registros guardados: {len(clean_records)}")
    except Exception as e:
        logging.error(f"[ETL LUGARES] Falla catastrófica de base de datos en SQLite: {str(e)}")
        return {
            'success': False,
            'message': f'Falla catastrófica de base de datos en SQLite: {str(e)}',
            'original_count': original_count,
            'cleaned_count': 0
        }
        
    # Estructurar mapeo visual para el log de auditoría
    mapped_columns = {}
    for key, idx in indices.items():
        if idx is not None and idx < len(first_line_fields):
            mapped_columns[key] = first_line_fields[idx] if has_headers else f"Columna {idx}"
        else:
            mapped_columns[key] = "No asignada"
            
    logging.info(f"[ETL LUGARES] ETL completado. Originales: {original_count}, Guardados: {len(clean_records)}, Corruptos: {len(corrupt_records)}, Duplicados: {len(duplicate_records)}")
    return {
        'success': True,
        'delimiter_detected': delimiter,
        'has_headers': has_headers,
        'mapped_columns': mapped_columns,
        'original_count': original_count,
        'cleaned_count': len(clean_records),
        'corrupt_count': len(corrupt_records),
        'duplicate_count': len(duplicate_records),
        'clean_records': clean_records,
        'corrupt': corrupt_records,
        'duplicates': duplicate_records
    }
