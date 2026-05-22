# Sistema Web de ETL y Normalización Dinámica en 3FN

Este es un proyecto completo, modular y listo para **evaluaciones de Arquitectura y Almacenamiento de Datos / Procesamiento ETL / Normalización de Datasets**.

La aplicación proporciona una interfaz web interactiva y moderna (diseño futurista oscuro con *glassmorphism*) construida sobre una arquitectura liviana en **Python (Flask)** y **SQLite**, que permite importar, limpiar, normalizar en **Tercera Forma Normal (3FN)** y almacenar cualquier archivo de datos planos (`.txt`, `.csv`) de forma totalmente dinámica y adaptativa.

---

## 🎯 Características y Capacidades de Ingeniería de Datos

1. **Lectura y Detección Estructural Dinámica**: 
   - Analiza en caliente los archivos para identificar de forma estadística el delimitador más óptimo (`,`, `;`, `\t`, `|`).
   - Mapea de forma visual y dinámica las columnas en la interfaz web por medio de un sistema heurístico de auto-selección o por personalización del usuario. **No está hardcodeado para un archivo fijo.**

2. **Procesamiento de Famosos y Fechas (Parte 1)**:
   - **Limpieza de Caracteres Basura**: Elimina selectivamente caracteres no permitidos (`@ # ; : | / \ *`) en campos textuales.
   - **Motor de Unificación de Fechas**: Unifica formatos mixtos (`YYYY/MM/DD`, `MM-DD-YYYY`, `DD/MM/YYYY`, `YYYY-MM-DD`, y fechas mezcladas dentro de cadenas de texto) al estándar chileno **`DD-MM-YYYY`**.
   - **Detección Avanzada de Duplicados**: Omitiendo registros basándose en llaves exactas y coincidencias ortográficas parciales (similitud de strings >85%) con la misma fecha de nacimiento.
   - **Cálculos Automáticos**: Edad calculada de manera dinámica en caliente con activación del flag booleano de cumpleaños (`cumpleanos_flag` = TRUE si el mes y día coinciden con el sistema actual).

3. **Normalización Relacional de Lugares en 3FN (Parte 2)**:
   - Toma el dataset plano de lugares y rompe las redundancias separando la información limpia en **3 tablas relacionales en Tercera Forma Normal (3FN)**:
     - **`Lugares`**: Entidad maestra (ID y nombre único).
     - **`Georeferencias`**: Coordenadas geográficas (`latitud`, `longitud`) con relación 1:1 y llaves foráneas. Incluye validación física de límites (-90 a 90 para latitud, -180 a 180 para longitud).
     - **`Direcciones`**: Detalles de dirección física (`calle`, `número`, `ciudad`, `país`) con relación 1:N.

4. **Interfaz de Usuario de Alta Gama (UI/UX)**:
   - **Estética Ultra Premium**: Panel de control con tema oscuro profundo, gradientes de luz neon, paneles semi-transparentes (*glassmorphic*), y tipografías estilizadas de Google Fonts (*Outfit* e *Inter*).
   - **Asistente de Mapeo Dinámico**: Permite previsualizar la estructura del archivo subido en segundos.
   - **Dashboard Interactivos**: Pestañas de resultados en caliente (KPIs de procesamiento, tablas de datos limpios, tablas con auditoría de por qué se rechazó o catalogó como corrupto un registro, duplicados omitidos, y previsualización en vivo del código SQL relacional generado).
   - **Descargas Integradas**: Exportación a CSV limpios y descarga directa del archivo `schema.sql` generado de forma dinámica.

---

## 🏗️ Arquitectura del Proyecto

El código está estructurado de forma modular y documentado exhaustivamente para facilitar su análisis docente:

```text
evaluacion 2 avance 2/
│
├── app.py                  # Controlador Web Flask (Servicio API y ruteador principal)
├── database.py             # Capa de almacenamiento SQLite y exportador de scripts SQL
├── etl_famosos.py          # Módulo ETL Parte 1: Limpieza, deduplicación y cálculo de Famosos
├── etl_lugares.py          # Módulo ETL Parte 2: Descomposición relacional en 3FN de Lugares
├── utils.py                # Utilidades: unificador de fechas, limpiador de texto y detector de separadores
│
├── requirements.txt        # Dependencias de librerías del proyecto
├── generate_test_data.py   # Script automatizado para recrear datasets sucios de prueba
├── DATOS2026-2.TXT         # Dataset generado de prueba para Famosos (sucio)
├── DATOS3.TXT              # Dataset generado de prueba para Lugares (sucio)
│
├── templates/
│   └── index.html          # Interfaz web responsiva y semántica SEO
└── static/
    ├── css/
    │   └── style.css       # Estilos visuales premium, animaciones y glassmorphism
    └── js/
        └── app.js          # Controladores AJAX, mapeador heurístico y dibujado dinámico de tablas
```

---

## 🚀 Guía de Ejecución Paso a Paso

Sigue estos simples pasos para iniciar la aplicación localmente en cualquier sistema con Python 3 instalado:

### 1. Clonar o Ubicar el Directorio
Abre una consola (PowerShell en Windows o Terminal en Mac/Linux) en la carpeta del proyecto:
```bash
cd "c:\Users\Usuario\Desktop\evaluacion 2 avance 2"
```

### 2. Instalar Dependencias
Instala la biblioteca Flask listada en `requirements.txt` (el proyecto usa librerías nativas estándar para asegurar 100% de portabilidad en la corrección):
```bash
pip install -r requirements.txt
```

### 3. Generar los Datasets de Prueba Académicos
Ejecuta el script generador de datos sucios para crear instantáneamente archivos con los que probar el sistema:
```bash
python generate_test_data.py
```
*(Esto creará `DATOS2026-2.TXT` y `DATOS3.TXT` con fechas incorrectas, duplicados fonéticos, caracteres basura y registros corruptos).*

### 4. Lanzar el Servidor Web
Arranca la aplicación Flask:
```bash
python app.py
```

### 5. Abrir la Aplicación
Abre tu navegador de preferencia e ingresa a la siguiente URL local:
```html
http://localhost:5000
```

---

## 🗄️ Diseño Físico de Base de Datos (SQLite)

El motor ETL autogenera la base de datos `etl_evaluacion.db` y mantiene las siguientes estructuras DDL normalizadas:

```sql
-- 1. ENTIDAD DE FAMOSOS (Parte 1)
CREATE TABLE Famosos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    fecha_nacimiento TEXT NOT NULL,
    edad INTEGER NOT NULL,
    cumpleanos_flag INTEGER NOT NULL -- 1 = TRUE, 0 = FALSE
);

-- 2. TABLA DE LUGARES (Parte 2: Entidad Fuerte)
CREATE TABLE Lugares (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre_lugar TEXT NOT NULL UNIQUE
);

-- 3. TABLA DE GEOREFERENCIAS (Parte 2: Relación 1:1 débil con Lugares)
CREATE TABLE Georeferencias (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lugar_id INTEGER UNIQUE,
    latitud REAL NOT NULL,
    longitud REAL NOT NULL,
    FOREIGN KEY (lugar_id) REFERENCES Lugares(id) ON DELETE CASCADE
);

-- 4. TABLA DE DIRECCIONES (Parte 2: Relación 1:N débil con Lugares)
CREATE TABLE Direcciones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lugar_id INTEGER,
    nombre_calle TEXT NOT NULL,
    numero_calle TEXT,
    ciudad_estado_provincia TEXT NOT NULL,
    pais TEXT NOT NULL,
    FOREIGN KEY (lugar_id) REFERENCES Lugares(id) ON DELETE CASCADE
);
```

---

## 💡 Conceptos Académicos Aplicados (Para la Evaluación)

Si el profesor pregunta por los fundamentos de ingeniería implementados, aquí están los puntos clave:
- **Cumplimiento de la 1FN (Primera Forma Normal)**: Se eliminaron los grupos repetitivos e inconsistentes. Los nombres y fechas están atomizados en campos atómicos limpios sin dobles significados.
- **Cumplimiento de la 2FN (Segunda Forma Normal)**: Todo atributo no clave depende funcionalmente de la llave primaria completa de la tabla. Al separar `Famosos` de `Lugares`, evitamos dependencias parciales.
- **Cumplimiento de la 3FN (Tercera Forma Normal)**: No existen dependencias transitivas. Los datos de georreferencia y direcciones físicas de los lugares dependen de la entidad `Lugares` por medio de una llave foránea indirecta, eliminando redundancia de almacenamiento (ej. si dos direcciones están en el mismo lugar, o si cambiara la latitud, no se altera el nombre del lugar).
- **Control de Inconsistencias**: En lugar de tirar una excepción y detener la aplicación, el motor de ETL aísla las líneas con fallas catastróficas en un búfer de auditoría (`corrupt_records`) para que el usuario pueda visualizar qué falló de forma amigable.
