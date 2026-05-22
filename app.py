# -*- coding: utf-8 -*-
"""
Servidor Web Flask - Controlador Principal
--------------------------------------------------------------------------------
Este archivo implementa el servidor web de la aplicación.
Proporciona las API REST para el análisis preliminar de archivos, la ejecución
de los pipelines ETL de famosos y lugares, la exportación de datos en formato
CSV y SQL, y la visualización interactiva de la base de datos SQLite.
"""

import json
import csv
import io
import os
from flask import Flask, request, jsonify, render_template, send_file, make_response

from database import (
    initialize_db,
    get_dashboard_stats,
    get_table_data,
    generate_sql_script,
    get_db_connection
)
from etl_famosos import process_famosos_etl
from etl_lugares import process_lugares_etl
from utils import detect_delimiter, clean_text_field, decode_content

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # Límite de carga: 16 MB

# Inicializar Base de Datos al arrancar el servidor
initialize_db()

@app.route('/')
def index():
    """
    Ruta principal. Carga la interfaz web del dashboard e inyecta las estadísticas básicas.
    """
    stats = get_dashboard_stats()
    return render_template('index.html', stats=stats)

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """
    Retorna estadísticas actualizadas de la base de datos para refrescar la UI.
    """
    stats = get_dashboard_stats()
    return jsonify(stats)

@app.route('/api/analyze-file', methods=['POST'])
def analyze_file():
    """
    Analiza un archivo subido de forma preliminar:
    - Detecta la codificación de caracteres.
    - Detecta el delimitador de forma dinámica.
    - Extrae la primera fila como cabeceras potenciales.
    - Genera una vista previa de las primeras 5 filas.
    - Facilita el mapeo de columnas dinámico en el Frontend.
    """
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No se encontró ningún archivo en la solicitud.'}), 400
        
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No se seleccionó ningún archivo.'}), 400
        
    try:
        # Leer el contenido del archivo en formato bytes
        content_bytes = file.read()
        
        # Decodificar usando nuestra utilidad multi-encoding
        content, encoding_detected = decode_content(content_bytes)
            
        delimiter = detect_delimiter(content)
        
        # Leer las primeras líneas usando StringIO
        f_in = io.StringIO(content.strip())
        reader = csv.reader(f_in, delimiter=delimiter)
        
        rows = []
        for i, row in enumerate(reader):
            if i >= 6:
                break
            rows.append(row)
            
        if not rows:
            return jsonify({'success': False, 'message': 'El archivo está vacío o no contiene líneas válidas.'}), 400
            
        headers = rows[0]
        preview_rows = rows[1:] if len(rows) > 1 else []
        
        return jsonify({
            'success': True,
            'filename': file.filename,
            'encoding': encoding_detected,
            'delimiter': delimiter,
            'headers': headers,
            'preview': preview_rows,
            'total_preview_lines': len(rows),
            'raw_content': content  # Retornamos el contenido para procesarlo de forma stateless
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error al analizar el archivo: {str(e)}'}), 500

def generate_audit_log(pipeline, result, encoding_name="DESCONOCIDO"):
    """
    Genera un log de auditoría en formato de consola detallado.
    Sigue una estética limpia y legible paso a paso.
    """
    logs = []
    logs.append("======================================================================")
    logs.append("                 LOG DE AUDITORÍA DEL PROCESO ETL")
    logs.append("======================================================================")
    logs.append(f"[INFO] Pipeline ejecutado: {pipeline.upper()}")
    logs.append(f"[INFO] Codificación del archivo: {encoding_name}")
    logs.append(f"[INFO] Delimitador estructural: '{result.get('delimiter_detected', 'N/A')}'")
    logs.append(f"[INFO] ¿Contiene cabeceras?: {'SÍ' if result.get('has_headers') else 'NO'}")
    
    mapped = result.get('mapped_columns', {})
    logs.append("[INFO] Mapeo de columnas detectado/aplicado:")
    for key, col in mapped.items():
        logs.append(f"       - {key}: {col}")
        
    logs.append(f"[INFO] Total filas a procesar en archivo: {result.get('original_count', 0)}")
    logs.append("----------------------------------------------------------------------")
    logs.append("[INFO] Iniciando lectura y normalización fila por fila...")
    
    # Agregar alertas sobre filas descartadas (corruptas)
    corrupts = result.get('corrupt', [])
    for c in corrupts:
        logs.append(f"[AVISO] Línea {c['line']} DESCARTADA - Razón: {c['reason']}")
        logs.append(f"        Contenido crudo: {c['raw']}")
        
    # Agregar alertas sobre duplicados
    duplicates = result.get('duplicates', [])
    for d in duplicates:
        orig = d.get('original', {})
        kept = d.get('kept', {})
        line_num = orig.get('line', 'N/A')
        reason = d.get('reason', 'Duplicado detectado')
        if pipeline == 'famosos':
            logs.append(f"[AVISO] Línea {line_num} DEDUPLICADA (Fuzzy) - Razón: {reason}")
            logs.append(f"        Valor descartado: {orig.get('nombre')} ({orig.get('fecha_nacimiento')})")
            if kept:
                logs.append(f"        Valor conservado: {kept.get('nombre')} ({kept.get('fecha_nacimiento')}) de la línea {kept.get('line')}")
        else:
            logs.append(f"[AVISO] Línea {line_num} DEDUPLICADA (3FN) - Razón: {reason}")
            logs.append(f"        Establecimiento descartado: '{orig.get('nombre_lugar')}' en {orig.get('direccion')}")
            
    logs.append("----------------------------------------------------------------------")
    logs.append("[INFO] Estado de la Base de Datos SQLite (Transacción Relacional):")
    if result.get('success'):
        logs.append("[OK]   Conexión establecida con etl_evaluacion.db.")
        if pipeline == 'famosos':
            logs.append(f"[OK]   Insertados exitosamente {result.get('cleaned_count', 0)} registros limpios en la tabla 'Famosos'.")
        else:
            logs.append("[OK]   Inserción relacional completa (3FN) en:")
            logs.append(f"       - Tabla 'Lugares': {result.get('cleaned_count', 0)} registros.")
            logs.append("       - Tablas 'Georeferencias' y 'Direcciones' enlazadas mediante llaves foráneas.")
        logs.append("[OK]   Transacción CONFIRMADA (Commit ejecutado con éxito).")
    else:
        logs.append("[ERROR] Transacción ABORTADA (Rollback realizado) debido a error en base de datos.")
        logs.append(f"[ERROR] Razón de la falla: {result.get('message', 'Desconocida')}")
        
    logs.append("======================================================================")
    logs.append("RESUMEN DE PROCESAMIENTO:")
    logs.append(f"       - Filas Originales: {result.get('original_count', 0)}")
    logs.append(f"       - Registros Limpios Insertados: {result.get('cleaned_count', 0)}")
    logs.append(f"       - Registros Corruptos Descartados: {result.get('corrupt_count', 0)}")
    logs.append(f"       - Registros Duplicados Descartados: {result.get('duplicate_count', 0)}")
    logs.append("======================================================================")
    
    return "\n".join(logs)

@app.route('/api/process-etl', methods=['POST'])
def process_etl():
    """
    Ejecuta el pipeline ETL seleccionado (Famosos o Lugares) sobre el archivo.
    Acepta mapeos dinámicos de columnas especificados por el usuario.
    Retorna un reporte detallado con log de auditoría para la consola web.
    """
    data = request.get_json()
    if not data or 'content' not in data or 'pipeline' not in data:
        return jsonify({'success': False, 'message': 'Datos incompletos para el procesamiento.'}), 400
        
    content = data['content']
    pipeline = data['pipeline']
    mapping = data.get('mapping', None)
    encoding_name = data.get('encoding', 'DESCONOCIDO')
    
    try:
        if pipeline == 'famosos':
            result = process_famosos_etl(content, mapping)
        elif pipeline == 'lugares':
            result = process_lugares_etl(content, mapping)
        else:
            return jsonify({'success': False, 'message': 'Pipeline de ETL no reconocido.'}), 400
            
        # Refrescar automáticamente el script schema.sql con los datos más recientes cargados
        generate_sql_script()
        
        # Generar el log de auditoría detallado para la interfaz web
        audit_log_text = generate_audit_log(pipeline, result, encoding_name)
        result['audit_log'] = audit_log_text
        
        return jsonify(result)
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': f'Error en la ejecución del ETL: {str(e)}',
            'audit_log': f"[ERROR] Falla crítica e inesperada en el pipeline ETL:\n{str(e)}"
        }), 500

@app.route('/api/database', methods=['GET'])
def get_db_explorer():
    """
    API Exploradora de la Base de Datos.
    Retorna el contenido actual de las tablas relacionales para mostrarlas en el Dashboard.
    """
    table = request.args.get('table', 'Famosos')
    valid_tables = ['Famosos', 'Lugares', 'Georeferencias', 'Direcciones']
    
    if table not in valid_tables:
        return jsonify({'success': False, 'message': f'Tabla "{table}" no es válida.'}), 400
        
    try:
        data = get_table_data(table)
        return jsonify({
            'success': True,
            'table': table,
            'data': data
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error al consultar base de datos: {str(e)}'}), 500

@app.route('/api/download/csv', methods=['POST'])
def download_csv():
    """
    Genera un archivo CSV limpio consolidado a partir de los datos procesados en la UI.
    """
    try:
        data = request.get_json()
        if not data or 'records' not in data or 'fields' not in data:
            return jsonify({'success': False, 'message': 'Estructura de datos inválida para CSV.'}), 400
            
        records = data['records']
        fields = data['fields']
        filename = data.get('filename', 'dataset_limpio.csv')
        
        si = io.StringIO()
        cw = csv.writer(si, delimiter=';')
        
        # Escribir cabeceras
        cw.writerow(fields)
        
        # Escribir registros
        for r in records:
            row_data = [r.get(f, '') for f in fields]
            cw.writerow(row_data)
            
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = f"attachment; filename={filename}"
        output.headers["Content-type"] = "text/csv; charset=utf-8"
        return output
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error al exportar CSV: {str(e)}'}), 500

@app.route('/api/download/sql', methods=['GET'])
def download_sql():
    """
    Descarga el script schema.sql autogenerado con los CREATE TABLE y los datos reales cargados.
    """
    try:
        sql_path = 'schema.sql'
        if not os.path.exists(sql_path):
            # Si no existe, generarlo
            generate_sql_script()
            
        return send_file(
            sql_path,
            as_attachment=True,
            download_name='schema.sql',
            mimetype='application/sql'
        )
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error al descargar SQL: {str(e)}'}), 500

@app.route('/api/clear-db', methods=['POST'])
def clear_database():
    """
    Limpia por completo las tablas de la base de datos. Utilidad de reinicio rápido.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Direcciones;")
        cursor.execute("DELETE FROM Georeferencias;")
        cursor.execute("DELETE FROM Lugares;")
        cursor.execute("DELETE FROM Famosos;")
        conn.commit()
        conn.close()
        
        # Regenerar schema.sql vacío
        generate_sql_script()
        
        return jsonify({'success': True, 'message': 'Base de datos vaciada con éxito.'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error al limpiar base de datos: {str(e)}'}), 500

@app.route('/api/logs', methods=['GET'])
def get_etl_logs():
    """
    Retorna las últimas 100 líneas del archivo logs/etl.log de forma segura.
    """
    log_path = os.path.join('logs', 'etl.log')
    if not os.path.exists(log_path):
        return jsonify({'success': True, 'logs': '[SISTEMA] No hay logs registrados aún.'})
        
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
        tail_lines = lines[-100:]
        return jsonify({
            'success': True,
            'logs': "".join(tail_lines)
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error al leer logs: {str(e)}'}), 500

if __name__ == '__main__':
    # Arranca el servidor Flask local en http://127.0.0.1:5000/
    print("=========================================================")
    print("  SERVIDOR ETL INICIADO: Abre http://localhost:5000")
    print("=========================================================")
    app.run(host='127.0.0.1', port=5000, debug=True)
