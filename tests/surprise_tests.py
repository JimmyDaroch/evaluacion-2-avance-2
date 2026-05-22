# -*- coding: utf-8 -*-
"""
Suite de Pruebas de Validación Final - Simulación de Pruebas de Profesor
----------------------------------------------------------------------
Este suite de pruebas ejecuta y valida los 11 escenarios académicos (TEST A hasta TEST K)
propuestos por el profesor para comprobar la robustez extrema ante datasets sorpresas.
"""

import unittest
import os
import sys

# Agregar el directorio raíz al path de Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import initialize_db, get_db_connection
from utils import decode_content, detect_delimiter, unify_date
from etl_famosos import process_famosos_etl
from etl_lugares import process_lugares_etl

class TestFinalAcademicSimulation(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Asegurar inicialización de base de datos
        initialize_db()
        
    def setUp(self):
        # Limpiar base de datos antes de cada test para aserciones independientes
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Direcciones;")
        cursor.execute("DELETE FROM Georeferencias;")
        cursor.execute("DELETE FROM Lugares;")
        cursor.execute("DELETE FROM Famosos;")
        conn.commit()
        conn.close()

    # =========================================================================
    # 1 — TESTING FAMOSOS
    # =========================================================================

    def test_a_delimitador_pipe(self):
        """TEST A — delimitador pipe y formatos variados YYYY/MM/DD e ISO"""
        content = (
            "nombre|fecha\n"
            "Tom Hanks|1956/07/09\n"
            "Angelina Jolie|1975-06-04"
        )
        result = process_famosos_etl(content)
        self.assertTrue(result['success'])
        self.assertEqual(result['delimiter_detected'], '|')
        self.assertEqual(result['cleaned_count'], 2)
        
        # Verificar unificación DD-MM-YYYY en BD
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT nombre, fecha_nacimiento FROM Famosos ORDER BY nombre;")
        rows = cursor.fetchall()
        conn.close()
        
        self.assertEqual(rows[0]['nombre'], "Angelina Jolie")
        self.assertEqual(rows[0]['fecha_nacimiento'], "04-06-1975")
        self.assertEqual(rows[1]['nombre'], "Tom Hanks")
        self.assertEqual(rows[1]['fecha_nacimiento'], "09-07-1956")

    def test_b_columnas_renombradas(self):
        """TEST B — columnas renombradas (celebrity, birthdate) y fechas verbales"""
        content = (
            "celebrity,birthdate\n"
            "Tom Hanks,July 9 1956\n"
            "Angelina Jolie,June 4 1975"
        )
        result = process_famosos_etl(content)
        self.assertTrue(result['success'])
        self.assertEqual(result['cleaned_count'], 2)
        
        # Verificar persistencia ordenada
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT nombre, fecha_nacimiento FROM Famosos ORDER BY nombre;")
        rows = cursor.fetchall()
        conn.close()
        
        self.assertEqual(rows[0]['nombre'], "Angelina Jolie")
        self.assertEqual(rows[0]['fecha_nacimiento'], "04-06-1975")
        self.assertEqual(rows[1]['nombre'], "Tom Hanks")
        self.assertEqual(rows[1]['fecha_nacimiento'], "09-07-1956")

    def test_c_dataset_sucio(self):
        """TEST C — dataset sucio con símbolos basura y filas desbordadas (ruido de estructura)"""
        content = (
            "nombre;fecha\n"
            "@@Tom Hanks###;1956///07///09\n"
            "Angelina Jolie;;;;1975-06-04" # Estructura con celdas intermedias vacías
        )
        result = process_famosos_etl(content)
        self.assertTrue(result['success'])
        
        # El primer registro se limpia y se guarda.
        # El segundo se detecta como corrupto porque el campo fecha (col 1) está vacío.
        self.assertEqual(result['cleaned_count'], 1)
        self.assertEqual(result['corrupt_count'], 1)
        
        # Verificar limpieza de basura en BD
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT nombre, fecha_nacimiento FROM Famosos;")
        row = cursor.fetchone()
        conn.close()
        
        self.assertEqual(row['nombre'], "Tom Hanks")
        self.assertEqual(row['fecha_nacimiento'], "09-07-1956")

    def test_d_duplicados_dificiles(self):
        """TEST D — duplicados difíciles con guiones, espacios y diferencias de case"""
        content = (
            "nombre|fecha\n"
            "Tom Hanks|09-07-1956\n"
            "tom hanks|09-07-1956\n"
            "TOM HANKS|09-07-1956\n"
            "Tom-Hanks|09-07-1956\n"
            "Tom_Hanks|09-07-1956"
        )
        result = process_famosos_etl(content)
        self.assertTrue(result['success'])
        self.assertEqual(result['cleaned_count'], 1)
        self.assertEqual(result['duplicate_count'], 4)

    def test_e_fechas_imposibles(self):
        """TEST E — fechas imposibles (31/02/1956, 99-99-9999) - Exclusión sin crasheos"""
        content = (
            "nombre|fecha\n"
            "Tom Hanks|31/02/1956\n"
            "Angelina Jolie|99-99-9999"
        )
        result = process_famosos_etl(content)
        self.assertTrue(result['success'])
        # Ambos registros son corruptos por fecha imposible
        self.assertEqual(result['cleaned_count'], 0)
        self.assertEqual(result['corrupt_count'], 2)

    def test_f_archivo_vacio(self):
        """TEST F — archivo vacío - Manejo seguro sin crashes"""
        content = ""
        result = process_famosos_etl(content)
        self.assertFalse(result['success'])
        self.assertIn("vacío", result['message'].lower())

    def test_g_encoding_utf16(self):
        """TEST G — encoding UTF-16 - Decodificación automática mediante BOM/bytes nulos"""
        raw_utf16 = "nombre;fecha\nKeanu Reeves;02-09-1964".encode('utf-16')
        
        # 1. Decodificar
        decoded, encoding = decode_content(raw_utf16)
        self.assertTrue(encoding.startswith('UTF-16'))
        
        # 2. Ejecutar ETL
        result = process_famosos_etl(decoded)
        self.assertTrue(result['success'])
        self.assertEqual(result['cleaned_count'], 1)

    # =========================================================================
    # 2 — TESTING LUGARES
    # =========================================================================

    def test_h_dataset_normal(self):
        """TEST H — lugares normal - División a 3FN en SQLite"""
        content = (
            "place;coord_x;coord_y;street;number;city;country\n"
            "Mall Plaza;-36.82;-73.04;Barros Arana;123;Concepcion;Chile"
        )
        result = process_lugares_etl(content)
        self.assertTrue(result['success'])
        self.assertEqual(result['cleaned_count'], 1)
        
        # Verificar separación de tablas relacionales (3FN)
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Tabla principal Lugares
        cursor.execute("SELECT * FROM Lugares;")
        lugares = cursor.fetchall()
        self.assertEqual(len(lugares), 1)
        self.assertEqual(lugares[0]['nombre_lugar'], "Mall Plaza")
        lugar_id = lugares[0]['id']
        
        # Tabla Georeferencias (1:1)
        cursor.execute("SELECT * FROM Georeferencias WHERE lugar_id = ?;", (lugar_id,))
        geos = cursor.fetchall()
        self.assertEqual(len(geos), 1)
        self.assertEqual(float(geos[0]['latitud']), -36.82)
        self.assertEqual(float(geos[0]['longitud']), -73.04)
        
        # Tabla Direcciones (1:N)
        cursor.execute("SELECT * FROM Direcciones WHERE lugar_id = ?;", (lugar_id,))
        dirs = cursor.fetchall()
        self.assertEqual(len(dirs), 1)
        self.assertEqual(dirs[0]['nombre_calle'], "Barros Arana")
        self.assertEqual(dirs[0]['numero_calle'], "123")
        self.assertEqual(dirs[0]['ciudad_estado_provincia'], "Concepcion")
        self.assertEqual(dirs[0]['pais'], "Chile")
        
        conn.close()

    def test_i_duplicados_lugares(self):
        """TEST I — duplicados de lugares - Deduplicación lógica por clave compuesta"""
        content = (
            "place;coord_x;coord_y;street;number;city;country\n"
            "Mall Plaza;-36.82;-73.04;Barros Arana;123;Concepcion;Chile\n"
            "Mall Plaza;-36.82;-73.04;Barros Arana;123;Concepcion;Chile"
        )
        result = process_lugares_etl(content)
        self.assertTrue(result['success'])
        self.assertEqual(result['cleaned_count'], 1)
        self.assertEqual(result['duplicate_count'], 1)

    def test_j_coordenadas_invalidas(self):
        """TEST J — coordenadas inválidas (fuera de límites físicos -90/90 y -180/180)"""
        content = (
            "place;coord_x;coord_y;street;number;city;country\n"
            "Mall Plaza;999;-400;Barros Arana;123;Concepcion;Chile"
        )
        result = process_lugares_etl(content)
        self.assertTrue(result['success'])
        # Registro corrupto por coordenadas desbordadas
        self.assertEqual(result['cleaned_count'], 0)
        self.assertEqual(result['corrupt_count'], 1)

    def test_k_columnas_distintas(self):
        """TEST K — columnas con alias distintos (location, latitude, longitude, address, number, province, country)"""
        content = (
            "location,latitude,longitude,address,number,province,country\n"
            "Mall Plaza,-36.82,-73.04,Barros Arana,123,Concepcion,Chile"
        )
        result = process_lugares_etl(content)
        self.assertTrue(result['success'])
        self.assertEqual(result['cleaned_count'], 1)
        
        # Verificar mapeo correcto
        self.assertEqual(result['mapped_columns']['lugar_idx'], "location")
        self.assertEqual(result['mapped_columns']['lat_idx'], "latitude")
        self.assertEqual(result['mapped_columns']['lon_idx'], "longitude")
        self.assertEqual(result['mapped_columns']['calle_idx'], "address")

if __name__ == '__main__':
    unittest.main()
