# -*- coding: utf-8 -*-
"""
Script de Generación de Datos de Prueba Académicos
--------------------------------------------------------------------------------
Este script genera automáticamente archivos de prueba sucios y con anomalías
estructurales y de contenido para simular los archivos que usará el profesor.
Crea:
- DATOS2026-2.TXT (Parte 1: Famosos con fechas variadas, basura textual y duplicados)
- DATOS3.TXT (Parte 2: Lugares con coordenadas, direcciones y duplicados lógicos)
"""

import os
from datetime import datetime

def generate_data():
    # Obtener la fecha actual del sistema para inyectar una fecha de cumpleaños hoy
    today = datetime.now()
    hoy_dia = today.strftime("%d")
    hoy_mes = today.strftime("%m")
    
    # 1. GENERAR DATOS2026-2.TXT (Famosos)
    famosos_content = [
        "Famoso_Nombre|Fecha_Nacimiento_Inconsistente",
        "Pedro Pascal|1975-04-02",
        "Keanu @Reeves#|02/09/1964",
        "Shakira Mebarak|02-02-1977",
        f"Daniel Muñoz|{hoy_dia}-{hoy_mes}-1966",  # Cumpleaños Hoy!
        "Alexis Sánchez*|Nacido el 19 de diciembre de 1988 en Tocopilla", # Texto Mezclado
        "Cecilia Bolocco;|1965/05/19",
        "Pedro Pascal|1975-04-02",  # Duplicado Exacto
        "Keanu Reeves|02-09-1964",  # Duplicado Ortográfico/Parcial (con diferente limpieza)
        "Fila Corrupta Sin Fecha",   # Corrupto: Falta columna
        "Famoso Fantasma|fecha_invalida_total_123" # Corrupto: Fecha incomprensible
    ]
    
    famosos_path = 'DATOS2026-2.TXT'
    with open(famosos_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(famosos_content))
    print(f"[OK] Archivo de Famosos generado con exito: '{famosos_path}' ({len(famosos_content)-1} registros)")
    
    # 2. GENERAR DATOS3.TXT (Lugares)
    lugares_content = [
        "Lugar_Nombre;Latitud;Longitud;Calle_Nombre;Numero;Ciudad_Comuna;Pais",
        "Parque O'Higgins;-33.456;-70.648;Avenida Beauchef;399;Santiago;Chile",
        "Torre Entel#;-33.444;-70.653;Amunátegui;20;Santiago;Chile",
        "Teatro Municipal;-33.441;-70.647;Agustinas;794;Santiago;Chile",
        "Teatro Municipal;-33.441;-70.647;Agustinas;794;Santiago;Chile", # Duplicado Lógico
        ";33.441;-70.647;Agustinas;794;Santiago;Chile",                 # Corrupto: Falta nombre lugar
        "Lugar Erratico;125.40;-70.647;Agustinas;794;Santiago;Chile",    # Corrupto: Latitud fuera de rango físico
        "Valparaíso Port;-33.037;-71.624;Avenida Errázuriz;1200;Valparaíso;Chile",
        "La Moneda Palace;-33.443;-70.653;Moneda;S/N;Santiago;Chile"
    ]
    
    lugares_path = 'DATOS3.TXT'
    with open(lugares_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lugares_content))
    print(f"[OK] Archivo de Lugares generado con exito: '{lugares_path}' ({len(lugares_content)-1} registros)")

if __name__ == '__main__':
    generate_data()
