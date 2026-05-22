# -*- coding: utf-8 -*-
"""
Script de Generación de Datasets Sucios de Prueba
--------------------------------------------------------------------------------
Este script autogenera archivos rotos y atípicos en la carpeta /tests para
validar la robustez extrema de los pipelines ETL.
"""

import os

def generate_test_files():
    # Crear carpeta tests si no existe
    os.makedirs('tests', exist_ok=True)
    
    # 1. Dataset de Famosos altamente sucio (famosos_dirty.txt)
    # Delimitado por Pipe (|), Codificado en Latin-1 (ISO-8859-1) para probar encoding,
    # contiene caracteres prohibidos, fechas ambiguas, verbales, duplicados fuzzy y filas vacías/corruptas.
    famosos_content = (
        "celebridad|nacido\n"
        "P#e@d;ro Pa/s\\c*al|Nacido el 2 de abril de 1975\n"
        "  Pedro Pascal |1975-04-02\n"  # Duplicado exacto post-fuzzy trim
        "PEDRO pascal|02-04-1975\n"    # Duplicado fuzzy (case-insensitive)
        "Keanu Reeves|September 2, 1964\n"
        "keanu  reeves|2 Sep 1964\n"    # Duplicado fuzzy (doble espacio y minúsculas)
        "Penélope Cruz|28 de abril de 1974\n"
        "Penelope Cruz|28-04-1974\n"   # Duplicado fuzzy (quitar tildes)
        "Tom-Hanks|9 Jul 1956\n"        # Tom Hanks con guion
        "Tom_Hanks|1956-07-09\n"       # Duplicado fuzzy (con guion bajo)
        "Tom Hanks|09-07-1956\n"       # Duplicado fuzzy (con espacio)
        "Fila_Rota_Sin_Columnas\n"      # Línea corrupta (estructura incompleta)
        "Famoso Sin Fecha|\n"           # Línea corrupta (fecha vacía)
        "Famoso Con Fecha Invalida|esto-no-es-una-fecha\n" # Línea corrupta (fecha no parseable)
        "Scarlett Johansson|1984/11/22\n"
    )
    
    famosos_path = os.path.join('tests', 'famosos_dirty.txt')
    with open(famosos_path, 'w', encoding='latin-1') as f:
        f.write(famosos_content)
        
    print(f"[OK] Generado dataset sucio: {famosos_path} (Latin-1)")
    
    # 2. Dataset de Lugares con coordenadas traspuestas y nombres atípicos (lugares_swapped.csv)
    # Delimitado por punto y coma (;), contiene alias (coord_x, coord_y),
    # coordenadas en extremos de rango, y una latitud fuera de límite físico (95.0) para descartar.
    lugares_content = (
        "place;coord_y;coord_x;street;altura;provincia;country\n"
        "Torre Entel;-70.6536;-33.4444;Amunátegui;20;Santiago;Chile\n"
        "Costanera Center;-70.6061;-33.4189;Avenida Andrés Bello;2425;Providencia;Chile\n"
        "Torre Entel  ;-70.6536;-33.4444;Amunategui;20;Santiago;Chile\n" # Duplicado exacto
        "Lugar Latitud Excedida;-70.6500;95.0;Calle Falsa;123;Santiago;Chile\n" # Corrupto (lat > 90)
        "Lugar Longitud Excedida;-190.0;-33.4400;Calle Falsa;456;Santiago;Chile\n" # Corrupto (lon < -180)
        "Lugar Sin Nombre;;-33.4400;Calle Falsa;789;Santiago;Chile\n" # Corrupto (lugar vacío)
        "La Moneda;-70.6538;-33.4429;Moneda;s/n;Santiago;Chile\n"
    )
    
    lugares_path = os.path.join('tests', 'lugares_swapped.csv')
    with open(lugares_path, 'w', encoding='utf-8') as f:
        f.write(lugares_content)
        
    print(f"[OK] Generado dataset sucio: {lugares_path} (UTF-8)")

if __name__ == '__main__':
    generate_test_files()
