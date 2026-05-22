# -*- coding: utf-8 -*-
"""
Módulo de Base de Datos - Sistema ETL y Normalización
--------------------------------------------------------------------------------
Este archivo gestiona la base de datos SQLite del proyecto. Contiene la creación
de los esquemas relacionales (Parte 1 y Parte 2), las transacciones de guardado
de datos normalizados, la obtención de métricas y la autogeneración del script SQL.
"""

import sqlite3
import os

DATABASE_PATH = 'etl_evaluacion.db'

def get_db_connection():
    """
    Retorna una conexión activa a la base de datos SQLite con soporte para Row.
    """
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    # Habilitar soporte para llaves foráneas en SQLite
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def initialize_db():
    """
    Inicializa el esquema de base de datos creando todas las tablas requeridas.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Tabla de Famosos (Parte 1)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Famosos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        fecha_nacimiento TEXT NOT NULL,
        edad INTEGER NOT NULL,
        cumpleanos_flag INTEGER NOT NULL -- 1 = TRUE, 0 = FALSE
    );
    """)
    
    # 2. Tablas Normalizadas de Lugares (Parte 2 en 3FN con Relaciones)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Lugares (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre_lugar TEXT NOT NULL UNIQUE
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Georeferencias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lugar_id INTEGER UNIQUE,
        latitud REAL NOT NULL,
        longitud REAL NOT NULL,
        FOREIGN KEY (lugar_id) REFERENCES Lugares(id) ON DELETE CASCADE
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Direcciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lugar_id INTEGER,
        nombre_calle TEXT NOT NULL,
        numero_calle TEXT,
        ciudad_estado_provincia TEXT NOT NULL,
        pais TEXT NOT NULL,
        FOREIGN KEY (lugar_id) REFERENCES Lugares(id) ON DELETE CASCADE
    );
    """)
    
    conn.commit()
    conn.close()

def save_famosos(records):
    """
    Limpia la tabla Famosos e inserta un conjunto de registros procesados por el ETL.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Limpieza de ejecuciones anteriores
    cursor.execute("DELETE FROM Famosos;")
    
    # Inserción masiva segura usando parámetros parametrizados
    for r in records:
        cursor.execute("""
        INSERT INTO Famosos (nombre, fecha_nacimiento, edad, cumpleanos_flag)
        VALUES (?, ?, ?, ?);
        """, (r['nombre'], r['fecha_nacimiento'], r['edad'], 1 if r['cumpleanos_flag'] else 0))
        
    conn.commit()
    conn.close()

def save_lugares_relacional(records):
    """
    Normaliza y guarda la estructura en las 3 tablas relacionales (Lugares, Georeferencias y Direcciones).
    Limpia las tablas de ejecuciones anteriores y mantiene integridad relacional.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Limpieza previa en orden inverso de dependencias para evitar violaciones de llave foránea
        cursor.execute("DELETE FROM Direcciones;")
        cursor.execute("DELETE FROM Georeferencias;")
        cursor.execute("DELETE FROM Lugares;")
        
        # Procesar e insertar registros de forma normalizada
        for r in records:
            # 1. Insertar o recuperar el ID del Lugar principal
            nombre_lugar = r['nombre_lugar']
            cursor.execute("SELECT id FROM Lugares WHERE nombre_lugar = ?;", (nombre_lugar,))
            row = cursor.fetchone()
            
            if row:
                lugar_id = row[0]
            else:
                cursor.execute("INSERT INTO Lugares (nombre_lugar) VALUES (?);", (nombre_lugar,))
                lugar_id = cursor.lastrowid
                
            # 2. Insertar Georeferencias (1:1 o 1:N por Lugar)
            # Validamos si existen datos de latitud/longitud
            if r.get('latitud') is not None and r.get('longitud') is not None:
                # Comprobar si ya existe una georreferencia para este lugar
                cursor.execute("SELECT id FROM Georeferencias WHERE lugar_id = ?;", (lugar_id,))
                if not cursor.fetchone():
                    cursor.execute("""
                    INSERT INTO Georeferencias (lugar_id, latitud, longitud)
                    VALUES (?, ?, ?);
                    """, (lugar_id, float(r['latitud']), float(r['longitud'])))
            
            # 3. Insertar Direcciones (1:N)
            cursor.execute("""
            INSERT INTO Direcciones (lugar_id, nombre_calle, numero_calle, ciudad_estado_provincia, pais)
            VALUES (?, ?, ?, ?, ?);
            """, (
                lugar_id,
                r.get('nombre_calle', 'Sin Nombre'),
                r.get('numero_calle', ''),
                r.get('ciudad_estado_provincia', 'Desconocido'),
                r.get('pais', 'Chile')
            ))
            
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_table_data(table_name):
    """
    Retorna todos los registros de una tabla específica como diccionarios.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name};")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_dashboard_stats():
    """
    Calcula estadísticas agregadas generales sobre la base de datos para mostrarlas en el Home.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    stats = {}
    try:
        cursor.execute("SELECT COUNT(*) FROM Famosos;")
        stats['total_famosos'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM Famosos WHERE cumpleanos_flag = 1;")
        stats['cumpleañeros'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM Lugares;")
        stats['total_lugares'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM Georeferencias;")
        stats['total_georeferencias'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM Direcciones;")
        stats['total_direcciones'] = cursor.fetchone()[0]
    except sqlite3.OperationalError:
        stats = {
            'total_famosos': 0, 'cumpleañeros': 0,
            'total_lugares': 0, 'total_georeferencias': 0, 'total_direcciones': 0
        }
    finally:
        conn.close()
        
    return stats

def generate_sql_script(output_path='schema.sql'):
    """
    Genera de forma completamente dinámica un archivo SQL autocontenido (.sql)
    que incluye las instrucciones DDL (CREATE TABLE) y DML (INSERT) reales con
    los datos limpios guardados en la base de datos.
    """
    sql_content = []
    sql_content.append("/*\n"
                       "========================================================================\n"
                       "SCRIPT SQL DE MIGRACION Y BASE DE DATOS AUTO-GENERADO POR EL MOTOR ETL\n"
                       "Evaluación Académica: Arquitectura y Almacenamiento de Datos\n"
                       "========================================================================\n"
                       "*/\n\n")
    
    sql_content.append("-- Habilitar restricción de llaves foráneas\n"
                       "PRAGMA foreign_keys = ON;\n\n"
                       "DROP TABLE IF EXISTS Direcciones;\n"
                       "DROP TABLE IF EXISTS Georeferencias;\n"
                       "DROP TABLE IF EXISTS Lugares;\n"
                       "DROP TABLE IF EXISTS Famosos;\n\n"
                       "-- =====================================================================\n"
                       "-- DDL - CREACIÓN DE TABLAS Y RELACIONES\n"
                       "-- =====================================================================\n\n")
    
    # Esquema Famosos
    sql_content.append("CREATE TABLE Famosos (\n"
                       "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                       "    nombre TEXT NOT NULL,\n"
                       "    fecha_nacimiento TEXT NOT NULL,\n"
                       "    edad INTEGER NOT NULL,\n"
                       "    cumpleanos_flag INTEGER NOT NULL -- 1 = TRUE, 0 = FALSE\n"
                       ");\n\n")
    
    # Esquema Lugares
    sql_content.append("CREATE TABLE Lugares (\n"
                       "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                       "    nombre_lugar TEXT NOT NULL UNIQUE\n"
                       ");\n\n")
    
    # Esquema Georeferencias
    sql_content.append("CREATE TABLE Georeferencias (\n"
                       "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                       "    lugar_id INTEGER UNIQUE,\n"
                       "    latitud REAL NOT NULL,\n"
                       "    longitud REAL NOT NULL,\n"
                       "    FOREIGN KEY (lugar_id) REFERENCES Lugares(id) ON DELETE CASCADE\n"
                       ");\n\n")
    
    # Esquema Direcciones
    sql_content.append("CREATE TABLE Direcciones (\n"
                       "    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                       "    lugar_id INTEGER,\n"
                       "    nombre_calle TEXT NOT NULL,\n"
                       "    numero_calle TEXT,\n"
                       "    ciudad_estado_provincia TEXT NOT NULL,\n"
                       "    pais TEXT NOT NULL,\n"
                       "    FOREIGN KEY (lugar_id) REFERENCES Lugares(id) ON DELETE CASCADE\n"
                       ");\n\n")
    
    sql_content.append("-- =====================================================================\n"
                       "-- DML - INSERCIÓN DE DATOS PROCESADOS (ETL)\n"
                       "-- =====================================================================\n\n")
    
    # Obtener datos e insertar
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Inserciones Famosos
    cursor.execute("SELECT * FROM Famosos;")
    famosos = cursor.fetchall()
    if famosos:
        sql_content.append("-- Datos Normalizados de Famosos\n")
        for f in famosos:
            nombre = f['nombre'].replace("'", "''")
            sql_content.append(f"INSERT INTO Famosos (id, nombre, fecha_nacimiento, edad, cumpleanos_flag) "
                               f"VALUES ({f['id']}, '{nombre}', '{f['fecha_nacimiento']}', {f['edad']}, {f['cumpleanos_flag']});\n")
        sql_content.append("\n")
        
    # Inserciones Lugares
    cursor.execute("SELECT * FROM Lugares;")
    lugares = cursor.fetchall()
    if lugares:
        sql_content.append("-- Datos Normalizados de Lugares\n")
        for l in lugares:
            nombre = l['nombre_lugar'].replace("'", "''")
            sql_content.append(f"INSERT INTO Lugares (id, nombre_lugar) VALUES ({l['id']}, '{nombre}');\n")
        sql_content.append("\n")
        
    # Inserciones Georeferencias
    cursor.execute("SELECT * FROM Georeferencias;")
    geos = cursor.fetchall()
    if geos:
        sql_content.append("-- Datos Normalizados de Georeferencias\n")
        for g in geos:
            sql_content.append(f"INSERT INTO Georeferencias (id, lugar_id, latitud, longitud) "
                               f"VALUES ({g['id']}, {g['lugar_id']}, {g['latitud']}, {g['longitud']});\n")
        sql_content.append("\n")
        
    # Inserciones Direcciones
    cursor.execute("SELECT * FROM Direcciones;")
    dirs = cursor.fetchall()
    if dirs:
        sql_content.append("-- Datos Normalizados de Direcciones\n")
        for d in dirs:
            calle = d['nombre_calle'].replace("'", "''")
            num = str(d['numero_calle']).replace("'", "''") if d['numero_calle'] else ''
            ciu = d['ciudad_estado_provincia'].replace("'", "''")
            pais = d['pais'].replace("'", "''")
            sql_content.append(f"INSERT INTO Direcciones (id, lugar_id, nombre_calle, numero_calle, ciudad_estado_provincia, pais) "
                               f"VALUES ({d['id']}, {d['lugar_id']}, '{calle}', '{num}', '{ciu}', '{pais}');\n")
        sql_content.append("\n")
        
    conn.close()
    
    # Escribir el script
    with open(output_path, 'w', encoding='utf-8') as f_out:
        f_out.write("".join(sql_content))
        
    return output_path
