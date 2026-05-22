# -*- coding: utf-8 -*-
"""
Suite de Pruebas Automatizadas de Integración ETL
--------------------------------------------------------------------------------
Este script realiza pruebas unitarias y de integración sobre los pipelines ETL
de Famosos y Lugares, utilizando los datasets altamente sucios generados.
"""

import unittest
import os
import sqlite3
import sys

# Agregar el directorio raíz al path de Python para importar correctamente
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import initialize_db, get_db_connection
from utils import decode_content, detect_delimiter, unify_date
from etl_famosos import process_famosos_etl
from etl_lugares import process_lugares_etl

class TestETLPipelines(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Asegurarse de que la base de datos esté inicializada
        initialize_db()
        
        # Generar los archivos sucios si no existen
        from tests.generate_dirty_tests import generate_test_files
        generate_test_files()
        
    def setUp(self):
        # Limpiar base de datos antes de cada prueba para tener aserciones exactas
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Direcciones;")
        cursor.execute("DELETE FROM Georeferencias;")
        cursor.execute("DELETE FROM Lugares;")
        cursor.execute("DELETE FROM Famosos;")
        conn.commit()
        conn.close()

    def test_01_utils_decoding(self):
        """Prueba que el decodificador iterativo sea resistente a codificaciones Latin-1"""
        famosos_path = os.path.join('tests', 'famosos_dirty.txt')
        with open(famosos_path, 'rb') as f:
            content_bytes = f.read()
            
        text, encoding = decode_content(content_bytes)
        self.assertEqual(encoding, 'LATIN-1')
        self.assertIn("Penélope Cruz", text)

    def test_02_utils_delimiter_detection(self):
        """Prueba la autodetección de delimitadores poco comunes"""
        # Caso Famosos: Delimitado por |
        famosos_path = os.path.join('tests', 'famosos_dirty.txt')
        with open(famosos_path, 'r', encoding='latin-1') as f:
            famosos_text = f.read()
        self.assertEqual(detect_delimiter(famosos_text), '|')
        
        # Caso Lugares: Delimitado por ;
        lugares_path = os.path.join('tests', 'lugares_swapped.csv')
        with open(lugares_path, 'r', encoding='utf-8') as f:
            lugares_text = f.read()
        self.assertEqual(detect_delimiter(lugares_text), ';')

    def test_03_utils_date_unification(self):
        """Prueba la unificación ultra-robusta de fechas de dateutil"""
        self.assertEqual(unify_date("2 de abril de 1975"), "02-04-1975")
        self.assertEqual(unify_date("1975-04-02"), "02-04-1975")
        self.assertEqual(unify_date("September 2, 1964"), "02-09-1964")
        self.assertEqual(unify_date("2 Sep 1964"), "02-09-1964")
        self.assertEqual(unify_date("28 de abril de 1974"), "28-04-1974")
        self.assertEqual(unify_date("1984/11/22"), "22-11-1984")

    def test_04_famosos_etl_resilience(self):
        """Prueba end-to-end del pipeline ETL de Famosos frente a datasets rotos"""
        famosos_path = os.path.join('tests', 'famosos_dirty.txt')
        with open(famosos_path, 'r', encoding='latin-1') as f:
            content = f.read()
            
        result = process_famosos_etl(content)
        
        # Aserciones estructurales del resultado
        self.assertTrue(result['success'])
        self.assertEqual(result['delimiter_detected'], '|')
        self.assertTrue(result['has_headers'])
        
        # KPIs esperados en famosos_dirty.txt
        # Filas de datos = 14 (la primera es cabecera: celebridad|nacido)
        self.assertEqual(result['original_count'], 14)
        self.assertEqual(result['cleaned_count'], 5)    # Pedro Pascal, Keanu Reeves, Penélope Cruz, Tom Hanks, Scarlett Johansson
        self.assertEqual(result['corrupt_count'], 3)    # Fila_Rota_Sin_Columnas, Famoso Sin Fecha, Famoso Con Fecha Invalida
        self.assertEqual(result['duplicate_count'], 6)  # Pedro Pascal trim, Pedro Pascal case, Keanu Reeves spaces, Penelope Cruz accents, Tom Hanks underscore, Tom Hanks space
        
        # Verificar inserciones en SQLite
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, nombre, fecha_nacimiento, edad, cumpleanos_flag FROM Famosos ORDER BY nombre;")
        rows = cursor.fetchall()
        conn.close()
        
        self.assertEqual(len(rows), 5)
        nombres = [r['nombre'] for r in rows]
        
        self.assertIn("Pedro Pascal", nombres)
        self.assertIn("Keanu Reeves", nombres)
        self.assertIn("Penelope Cruz", nombres) # Nombre normalizado (se remueven caracteres sucios pero mantiene texto limpio)
        self.assertIn("Tom-Hanks", nombres)
        self.assertIn("Scarlett Johansson", nombres)

    def test_05_lugares_etl_resilience(self):
        """Prueba end-to-end del pipeline ETL de Lugares con cabeceras cruzadas y límites físicos excedidos"""
        lugares_path = os.path.join('tests', 'lugares_swapped.csv')
        with open(lugares_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        result = process_lugares_etl(content)
        
        # Aserciones estructurales del resultado
        self.assertTrue(result['success'])
        self.assertEqual(result['delimiter_detected'], ';')
        self.assertTrue(result['has_headers'])
        
        # Verificar mapeo de coordenadas traspuestas
        # coord_x = latitud = index 2 en lugares_content
        # coord_y = longitud = index 1 en lugares_content
        self.assertEqual(result['mapped_columns']['lat_idx'], "coord_x")
        self.assertEqual(result['mapped_columns']['lon_idx'], "coord_y")
        
        # KPIs esperados en lugares_swapped.csv
        # Filas de datos = 7 (la primera es cabecera)
        self.assertEqual(result['original_count'], 7)
        self.assertEqual(result['cleaned_count'], 4)    # Torre Entel, Costanera Center, Lugar Sin Nombre, La Moneda
        self.assertEqual(result['corrupt_count'], 2)    # Lugar Latitud Excedida (95.0), Lugar Longitud Excedida (-190.0)
        self.assertEqual(result['duplicate_count'], 1)  # Torre Entel duplicado
        
        # Verificar inserciones relacionales 3FN en SQLite
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM Lugares;")
        lugares = cursor.fetchall()
        self.assertEqual(len(lugares), 4)
        
        cursor.execute("SELECT * FROM Georeferencias;")
        geos = cursor.fetchall()
        self.assertEqual(len(geos), 3)
        
        cursor.execute("SELECT * FROM Direcciones;")
        dirs = cursor.fetchall()
        self.assertEqual(len(dirs), 4)
        
        # Probar enlace de Llaves Foráneas (FK) de Torre Entel
        cursor.execute("""
            SELECT l.nombre_lugar, g.latitud, g.longitud, d.nombre_calle 
            FROM Lugares l
            JOIN Georeferencias g ON l.id = g.lugar_id
            JOIN Direcciones d ON l.id = d.lugar_id
            WHERE l.nombre_lugar = 'Torre Entel';
        """)
        torre = cursor.fetchone()
        conn.close()
        
        self.assertIsNotNone(torre)
        self.assertEqual(float(torre['latitud']), -33.4444)
        self.assertEqual(float(torre['longitud']), -70.6536)
        self.assertEqual(torre['nombre_calle'], "Amunategui") # Limpieza de tilde de Amunátegui

if __name__ == '__main__':
    unittest.main()
