/*
========================================================================
LÓGICA FRONTEND - SISTEMA ETL & NORMALIZACIÓN (STATLESS & DYNAMIC)
========================================================================
Controla subidas de archivos, mapeo de columnas, ejecución de pipelines,
pestañas de resultados, descargas de archivos y explorador de base de datos.
========================================================================
*/

// Cache global para evitar guardar archivos en disco del servidor (Estructura Stateless)
let fileCache = {
    famosos: null,
    lugares: null
};

// Almacenamiento local de los últimos resultados para descargas rápidas
let lastETLResult = {
    famosos: null,
    lugares: null
};

// Tabla activa en el explorador de base de datos
let currentExplorerTable = 'Famosos';

// Configuración de títulos de vistas
const viewTitles = {
    'dashboard': { title: 'Dashboard General', subtitle: 'Resumen estadístico y control de la base de datos relacional.' },
    'etl-famosos': { title: 'ETL Famosos y Fechas', subtitle: 'Normalización de fechas, cálculo de edades, flag de cumpleaños y eliminación de duplicados.' },
    'etl-lugares': { title: 'ETL Lugares en 3FN', subtitle: 'Normalización y partición relacional en 3 Tablas Normalizadas (3FN) con georeferenciación.' },
    'db-explorer': { title: 'Explorador de Base de Datos', subtitle: 'Consulta directa y visualización de las tablas de SQLite en tiempo real.' },
    'etl-logs': { title: 'Registro de Actividad (Logs)', subtitle: 'Historial de procesamiento y auditoría persistente del servidor.' }
};

// Ejecución al cargar el DOM
document.addEventListener('DOMContentLoaded', () => {
    setupDragAndDrop('drop-zone-famosos', 'file-famosos', 'famosos');
    setupDragAndDrop('drop-zone-lugares', 'file-lugares', 'lugares');
    
    // Cargar estadísticas iniciales
    refreshStats();
});

// Navegación entre Pestañas Principales (Sidebar)
function switchTab(tabId) {
    // Actualizar botones de navegación
    document.querySelectorAll('.nav-item').forEach(btn => btn.classList.remove('active'));
    event.currentTarget.classList.add('active');
    
    // Ocultar todas las vistas
    document.querySelectorAll('.tab-pane').forEach(view => view.classList.remove('active'));
    
    // Mostrar la vista seleccionada
    const targetView = document.getElementById(`view-${tabId}`);
    if (targetView) targetView.classList.add('active');
    
    // Actualizar Títulos de Cabecera
    if (viewTitles[tabId]) {
        document.getElementById('main-view-title').innerText = viewTitles[tabId].title;
        document.getElementById('main-view-subtitle').innerText = viewTitles[tabId].subtitle;
    }
    
    // Acciones especiales al entrar a vistas
    if (tabId === 'dashboard') {
        refreshStats();
    } else if (tabId === 'db-explorer') {
        loadTableData(currentExplorerTable);
    } else if (tabId === 'etl-logs') {
        loadETLLogs();
    }
}

// Configuración de Drag and Drop
function setupDragAndDrop(zoneId, inputId, pipelineType) {
    const dropZone = document.getElementById(zoneId);
    const fileInput = document.getElementById(inputId);
    
    if (!dropZone || !fileInput) return;
    
    // Evitar comportamientos por defecto del navegador
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, e => {
            e.preventDefault();
            e.stopPropagation();
        }, false);
    });
    
    // Iluminar zona al arrastrar
    ['dragenter', 'dragover'].forEach(eventName => {
        dropZone.addEventListener(eventName, () => {
            dropZone.classList.add('dragover');
        }, false);
    });
    
    ['dragleave', 'drop'].forEach(eventName => {
        dropZone.addEventListener(eventName, () => {
            dropZone.classList.remove('dragover');
        }, false);
    });
    
    // Al soltar el archivo
    dropZone.addEventListener('drop', e => {
        const dt = e.dataTransfer;
        const files = dt.files;
        if (files.length > 0) {
            fileInput.files = files;
            handleFileSelect(fileInput, pipelineType);
        }
    });
}

function triggerFileInput(inputId) {
    document.getElementById(inputId).click();
}

// Analizar archivo recién seleccionado (Carga preliminar)
function handleFileSelect(input, pipelineType) {
    const file = input.files[0];
    if (!file) return;
    
    const formData = new FormData();
    formData.append('file', file);
    
    // Mostrar estado de carga en la zona drag and drop
    const dropZone = document.getElementById(`drop-zone-${pipelineType}`);
    const originalHTML = dropZone.innerHTML;
    dropZone.innerHTML = `
        <div class="status-indicator" style="width: 24px; height: 24px;"></div>
        <p style="margin-top:10px;">Analizando estructura del dataset...</p>
    `;
    
    fetch('/api/analyze-file', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        // Restaurar zona de arrastre
        dropZone.innerHTML = originalHTML;
        
        if (!data.success) {
            alert(`Error: ${data.message}`);
            return;
        }
        
        // Guardar contenido del archivo en cache del cliente (Stateless)
        fileCache[pipelineType] = data.raw_content;
        fileCache[pipelineType + '_encoding'] = data.encoding; // Cachear encoding detectado
        
        // Cargar controles de mapeo dinámico
        setupColumnMapper(pipelineType, data.headers, data.delimiter);
    })
    .catch(error => {
        dropZone.innerHTML = originalHTML;
        console.error('Error:', error);
        alert('Falla crítica al intentar procesar el archivo en el servidor.');
    });
}

// Poblar los selects del mapeador dinámico con las columnas detectadas
function setupColumnMapper(pipeline, headers, delimiter) {
    const mapperBox = document.getElementById(`mapper-${pipeline}`);
    const infoText = document.getElementById(`mapper-info-${pipeline}`);
    
    if (!mapperBox) return;
    
    infoText.innerHTML = `Archivo cargado con éxito. Se detectó delimitador <strong>"${delimiter}"</strong> y un total de <strong>${headers.length} columnas</strong>.`;
    
    // Listar selects a configurar según el pipeline
    let selects = [];
    if (pipeline === 'famosos') {
        selects = ['map-famosos-nombre', 'map-famosos-fecha'];
    } else if (pipeline === 'lugares') {
        selects = [
            'map-lugares-nombre', 'map-lugares-lat', 'map-lugares-lon', 
            'map-lugares-calle', 'map-lugares-numero', 'map-lugares-ciudad', 'map-lugares-pais'
        ];
    }
    
    selects.forEach(selectId => {
        const select = document.getElementById(selectId);
        if (!select) return;
        
        select.innerHTML = '';
        
        // Opción vacía por defecto para campos no obligatorios
        if (selectId !== 'map-famosos-nombre' && selectId !== 'map-famosos-fecha' && selectId !== 'map-lugares-nombre') {
            const emptyOpt = document.createElement('option');
            emptyOpt.value = '';
            emptyOpt.text = '-- No Asignada (Auto / Defecto) --';
            select.appendChild(emptyOpt);
        }
        
        headers.forEach((header, index) => {
            const opt = document.createElement('option');
            opt.value = index;
            opt.text = `${header} (Columna ${index + 1})`;
            select.appendChild(opt);
        });
        
        // Ejecutar Heurísticas de Auto-selección inteligente
        autoSelectHeuristics(selectId, headers);
    });
    
    // Mostrar panel de mapeo con animación
    mapperBox.style.display = 'block';
    mapperBox.scrollIntoView({ behavior: 'smooth' });
}

// Algoritmo heurístico para pre-seleccionar columnas probables basados en palabras clave
function autoSelectHeuristics(selectId, headers) {
    const select = document.getElementById(selectId);
    const keywords = {
        'map-famosos-nombre': ['nombre', 'name', 'famoso', 'celebridad', 'persona'],
        'map-famosos-fecha': ['fecha', 'date', 'nacimiento', 'birth', 'cumple'],
        'map-lugares-nombre': ['lugar', 'place', 'sitio', 'nombre', 'name', 'establecimiento', 'atraccion'],
        'map-lugares-lat': ['lat', 'latitude', 'latitud', 'y'],
        'map-lugares-lon': ['lon', 'lng', 'longitude', 'longitud', 'x'],
        'map-lugares-calle': ['calle', 'street', 'direccion', 'address', 'via'],
        'map-lugares-numero': ['numero', 'num', 'number', 'nro', 'altura'],
        'map-lugares-ciudad': ['ciudad', 'city', 'provincia', 'estado', 'region', 'comuna'],
        'map-lugares-pais': ['pais', 'country', 'nacion']
    };
    
    const targets = keywords[selectId];
    if (!targets) return;
    
    for (let i = 0; i < headers.length; i++) {
        const headerLower = headers[i].toLowerCase();
        if (targets.some(keyword => headerLower.includes(keyword))) {
            select.value = i;
            break;
        }
    }
}

// Lanzar el procesamiento ETL enviando los datos crudos y mapeos configurados
function runETLPipeline(pipeline) {
    const content = fileCache[pipeline];
    if (!content) {
        alert("Suba un archivo antes de iniciar el procesamiento.");
        return;
    }
    
    let mapping = {};
    if (pipeline === 'famosos') {
        mapping = {
            'nombre_idx': document.getElementById('map-famosos-nombre').value,
            'fecha_idx': document.getElementById('map-famosos-fecha').value
        };
    } else if (pipeline === 'lugares') {
        mapping = {
            'lugar_idx': document.getElementById('map-lugares-nombre').value,
            'lat_idx': document.getElementById('map-lugares-lat').value,
            'lon_idx': document.getElementById('map-lugares-lon').value,
            'calle_idx': document.getElementById('map-lugares-calle').value,
            'numero_idx': document.getElementById('map-lugares-numero').value,
            'ciudad_idx': document.getElementById('map-lugares-ciudad').value,
            'pais_idx': document.getElementById('map-lugares-pais').value
        };
    }
    
    // Cambiar botón a estado de procesamiento
    const btn = event.currentTarget;
    const origText = btn.innerHTML;
    btn.innerHTML = `<span class="status-indicator"></span> Procesando ETL en base de datos...`;
    btn.disabled = true;
    
    fetch('/api/process-etl', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            content: content,
            pipeline: pipeline,
            mapping: mapping,
            encoding: fileCache[pipeline + '_encoding'] || 'DESCONOCIDO'
        })
    })
    .then(response => response.json())
    .then(data => {
        // Restaurar botón
        btn.innerHTML = origText;
        btn.disabled = false;
        
        if (!data.success) {
            // Incluso si el backend indica éxito: false (por ej. errores de DB), puede traer logs de auditoría
            if (data.audit_log) {
                renderETLResults(pipeline, data);
            }
            alert(`Error en ETL: ${data.message}`);
            return;
        }
        
        // Cachear resultados en cliente
        lastETLResult[pipeline] = data;
        
        // Renderizar reportes en caliente
        renderETLResults(pipeline, data);
        
        // Actualizar estadísticas globales del Home
        refreshStats();
    })
    .catch(error => {
        btn.innerHTML = origText;
        btn.disabled = false;
        console.error('Error:', error);
        alert('Falla crítica durante la ejecución del proceso ETL.');
    });
}

// Renderizar dinámicamente las pestañas de resultados e inyectar tablas
function renderETLResults(pipeline, data) {
    // 1. Rellenar Consola de Auditoría de Procesos
    const auditCard = document.getElementById(`audit-card-${pipeline}`);
    const auditTerminal = document.getElementById(`audit-log-terminal-${pipeline}`);
    if (auditCard && auditTerminal && data.audit_log) {
        auditTerminal.innerText = data.audit_log;
        auditCard.style.display = 'block';
        
        // Auto scroll en terminal
        setTimeout(() => {
            auditTerminal.scrollTop = auditTerminal.scrollHeight;
        }, 100);
    }

    // 2. Rellenar KPIs
    document.getElementById(`kpi-${pipeline.substring(0,3)}-total`).innerText = data.original_count || 0;
    document.getElementById(`kpi-${pipeline.substring(0,3)}-clean`).innerText = data.cleaned_count || 0;
    document.getElementById(`kpi-${pipeline.substring(0,3)}-dup`).innerText = data.duplicate_count || 0;
    document.getElementById(`kpi-${pipeline.substring(0,3)}-corr`).innerText = data.corrupt_count || 0;
    
    // Si no fue exitoso el ETL, omitimos renderizar tablas para evitar inconsistencias
    if (!data.success) {
        const cleanTbody = document.querySelector(`#table-${pipeline.substring(0,3)}-clean tbody`);
        if (cleanTbody) cleanTbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:var(--color-danger);">Falla en base de datos. Revisar consola de auditoría.</td></tr>`;
        return;
    }

    // 3. Renderizar tabla de limpios
    const cleanTbody = document.querySelector(`#table-${pipeline.substring(0,3)}-clean tbody`);
    cleanTbody.innerHTML = '';
    
    if (pipeline === 'famosos') {
        data.clean_records.forEach(r => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><span class="badge badge-success">Línea ${r.line}</span></td>
                <td><strong>${escapeHTML(r.nombre)}</strong></td>
                <td><span style="font-family: monospace;">${r.fecha_nacimiento}</span></td>
                <td>${r.edad} años</td>
                <td>${r.cumpleanos_flag ? '🎉 <span class="badge badge-success">SÍ CUMPLE HOY</span>' : '<span style="opacity:0.4;">No</span>'}</td>
            `;
            cleanTbody.appendChild(tr);
        });
    } else if (pipeline === 'lugares') {
        data.clean_records.forEach(r => {
            const tr = document.createElement('tr');
            const coords = r.latitud !== null ? `${r.latitud}, ${r.longitud}` : '<span style="opacity:0.4;">No Asignada</span>';
            const numText = r.numero_calle ? ` N° ${r.numero_calle}` : '';
            tr.innerHTML = `
                <td><span class="badge badge-success">Línea ${r.line}</span></td>
                <td><strong>${escapeHTML(r.nombre_lugar)}</strong></td>
                <td><span style="font-family: monospace;">${coords}</span></td>
                <td>${escapeHTML(r.nombre_calle)}${escapeHTML(numText)}</td>
                <td>${escapeHTML(r.ciudad_estado_provincia)}, <strong>${escapeHTML(r.pais)}</strong></td>
            `;
            cleanTbody.appendChild(tr);
        });
    }
    
    // 4. Renderizar duplicados
    const dupTbody = document.querySelector(`#table-${pipeline.substring(0,3)}-duplicates tbody`);
    dupTbody.innerHTML = '';
    
    if (!data.duplicates || data.duplicates.length === 0) {
        dupTbody.innerHTML = `<tr><td colspan="5" style="text-align:center; opacity:0.6;">Ningún registro duplicado detectado.</td></tr>`;
    } else {
        data.duplicates.forEach(d => {
            const tr = document.createElement('tr');
            if (pipeline === 'famosos') {
                tr.innerHTML = `
                    <td><span class="badge badge-warning">Línea ${d.original.line}</span></td>
                    <td>${escapeHTML(d.original.nombre)}</td>
                    <td>${d.original.fecha_nacimiento}</td>
                    <td><span class="badge badge-warning">${escapeHTML(d.reason)}</span></td>
                    <td>Línea ${d.kept.line} (${escapeHTML(d.kept.nombre)})</td>
                `;
            } else if (pipeline === 'lugares') {
                tr.innerHTML = `
                    <td><span class="badge badge-warning">Línea ${d.original.line}</span></td>
                    <td><strong>${escapeHTML(d.original.nombre_lugar)}</strong></td>
                    <td>${escapeHTML(d.original.direccion)}</td>
                    <td colspan="2"><span class="badge badge-warning">${escapeHTML(d.reason)}</span></td>
                `;
            }
            dupTbody.appendChild(tr);
        });
    }
    
    // 5. Renderizar corruptos
    const corrTbody = document.querySelector(`#table-${pipeline.substring(0,3)}-corrupt tbody`);
    corrTbody.innerHTML = '';
    
    if (!data.corrupt || data.corrupt.length === 0) {
        corrTbody.innerHTML = `<tr><td colspan="3" style="text-align:center; opacity:0.6;">Ningún registro corrupto detectado en las validaciones académicas.</td></tr>`;
    } else {
        data.corrupt.forEach(c => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><span class="badge badge-danger">Línea ${c.line}</span></td>
                <td style="font-family: monospace; font-size:0.8rem; background:rgba(239, 68, 68, 0.05);">${escapeHTML(c.raw)}</td>
                <td><strong style="color: var(--color-danger);">${escapeHTML(c.reason)}</strong></td>
            `;
            corrTbody.appendChild(tr);
        });
    }
    
    // 6. Cargar previsualización SQL en caliente
    fetch('/api/download/sql')
    .then(res => res.text())
    .then(sqlCode => {
        document.getElementById(`sql-${pipeline.substring(0,3)}-preview`).innerText = sqlCode;
    });
    
    // Mostrar sección completa de reportes
    const resultsBox = document.getElementById(`results-${pipeline}`);
    resultsBox.style.display = 'block';
    resultsBox.scrollIntoView({ behavior: 'smooth' });
}

// Navegación interna entre las pestañas del reporte ETL (Limpio/Corrupto/SQL)
function switchResultTab(pipelinePrefix, tabName) {
    const parentContainer = document.getElementById(`results-${pipelinePrefix === 'fam' ? 'famosos' : 'lugares'}`);
    
    // Desactivar botones de pestañas del reporte
    parentContainer.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    event.currentTarget.classList.add('active');
    
    // Desactivar todos los paneles
    parentContainer.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));
    
    // Activar panel seleccionado
    const targetPane = document.getElementById(`tab-${pipelinePrefix}-${tabName}`);
    if (targetPane) targetPane.classList.add('active');
}

// Descargar CSV Limpio generado al vuelo
function downloadCleanCSV(pipeline) {
    const data = lastETLResult[pipeline];
    if (!data || !data.clean_records || data.clean_records.length === 0) {
        alert('No hay registros limpios para descargar.');
        return;
    }
    
    let fields = [];
    let filename = '';
    
    if (pipeline === 'famosos') {
        fields = ['nombre', 'fecha_nacimiento', 'edad', 'cumpleanos_flag'];
        filename = 'famosos_normalizados_chile.csv';
    } else if (pipeline === 'lugares') {
        fields = ['nombre_lugar', 'latitud', 'longitud', 'nombre_calle', 'numero_calle', 'ciudad_estado_provincia', 'pais'];
        filename = 'lugares_normalizados_3fn.csv';
    }
    
    fetch('/api/download/csv', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            records: data.clean_records,
            fields: fields,
            filename: filename
        })
    })
    .then(res => res.blob())
    .then(blob => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
    })
    .catch(err => console.error('Error al exportar CSV:', err));
}

// Cargar y Browsar Datos reales de SQLite (Explorador DB)
function loadTableData(tableName, btnElement = null) {
    currentExplorerTable = tableName;
    
    if (btnElement) {
        document.querySelectorAll('.btn-pill').forEach(btn => btn.classList.remove('active'));
        btnElement.classList.add('active');
    }
    
    const thead = document.getElementById('thead-db-browser');
    const tbody = document.getElementById('tbody-db-browser');
    
    thead.innerHTML = '<tr><th>Cargando...</th></tr>';
    tbody.innerHTML = '<tr><td>Consultando base de datos física...</td></tr>';
    
    fetch(`/api/database?table=${tableName}`)
    .then(res => res.json())
    .then(resData => {
        if (!resData.success) {
            tbody.innerHTML = `<tr><td><strong style="color:var(--color-danger);">${resData.message}</strong></td></tr>`;
            return;
        }
        
        const data = resData.data;
        if (data.length === 0) {
            thead.innerHTML = '<tr><th>Tabla Vacía</th></tr>';
            tbody.innerHTML = `<tr><td style="opacity:0.6; text-align:center; padding: 40px 0;">La tabla <strong>${tableName}</strong> está vacía. Ejecute un pipeline de ETL correspondiente para poblarla.</td></tr>`;
            return;
        }
        
        // Obtener cabeceras a partir de las llaves del primer registro
        const headers = Object.keys(data[0]);
        
        thead.innerHTML = '';
        const trHead = document.createElement('tr');
        headers.forEach(h => {
            const th = document.createElement('th');
            th.innerText = h.toUpperCase();
            trHead.appendChild(th);
        });
        thead.appendChild(trHead);
        
        tbody.innerHTML = '';
        data.forEach(row => {
            const trBody = document.createElement('tr');
            headers.forEach(h => {
                const td = document.createElement('td');
                const val = row[h];
                if (h === 'cumpleanos_flag') {
                    td.innerHTML = val === 1 ? '🎉 <span class="badge badge-success">SÍ</span>' : '<span style="opacity:0.4;">No</span>';
                } else if (h === 'nombre' || h === 'nombre_lugar') {
                    td.innerHTML = `<strong>${escapeHTML(val)}</strong>`;
                } else {
                    td.innerText = val !== null ? val : '';
                }
                trBody.appendChild(td);
            });
            tbody.appendChild(trBody);
        });
    })
    .catch(err => {
        console.error(err);
        tbody.innerHTML = '<tr><td><strong style="color:var(--color-danger);">Error crítico al conectar con SQLite.</strong></td></tr>';
    });
}

function refreshCurrentTable() {
    loadTableData(currentExplorerTable);
}

// Vaciar Base de Datos Completa
function clearDatabase() {
    if (!confirm('🚨 ¿Está completamente seguro de vaciar todas las tablas de la base de datos relacional de SQLite? Se borrarán todos los registros cargados de famosos y lugares.')) {
        return;
    }
    
    fetch('/api/clear-db', {
        method: 'POST'
    })
    .then(res => res.json())
    .then(data => {
        alert(data.message);
        refreshStats();
        // Ocultar paneles de reportes previos cargados
        document.getElementById('results-famosos').style.display = 'none';
        document.getElementById('results-lugares').style.display = 'none';
        document.getElementById('mapper-famosos').style.display = 'none';
        document.getElementById('mapper-lugares').style.display = 'none';
        
        // Limpiar caches
        fileCache = { famosos: null, lugares: null };
        lastETLResult = { famosos: null, lugares: null };
        
        // Resetear inputs de archivos
        document.getElementById('file-famosos').value = '';
        document.getElementById('file-lugares').value = '';
    })
    .catch(err => console.error(err));
}

// Refrescar Estadísticas del Home (KPIs)
function refreshStats() {
    fetch('/api/stats')
    .then(res => res.json())
    .then(stats => {
        document.getElementById('stat-famosos').innerText = stats.total_famosos || 0;
        document.getElementById('stat-cumpleanos').innerText = stats.cumpleañeros || 0;
        document.getElementById('stat-lugares').innerText = stats.total_lugares || 0;
        document.getElementById('stat-direcciones').innerText = stats.total_direcciones || 0;
    })
    .catch(err => console.error('Error al actualizar estadísticas:', err));
}

// Utilidad simple para prevenir inyecciones HTML en tablas
function escapeHTML(str) {
    if (typeof str !== 'string') return str;
    return str.replace(/[&<>'"]/g, 
        tag => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            "'": '&#39;',
            '"': '&quot;'
        }[tag] || tag)
    );
}

// Cargar y mostrar los logs persistentes del servidor
function loadETLLogs() {
    const consoleElem = document.getElementById('etl-log-console');
    if (!consoleElem) return;
    
    consoleElem.innerText = 'Cargando logs del servidor...';
    
    fetch('/api/logs')
    .then(res => res.json())
    .then(data => {
        if (data.success) {
            consoleElem.innerText = data.logs || '[SISTEMA] No hay logs registrados aún.';
            // Desplazar automáticamente al final de los logs
            setTimeout(() => {
                consoleElem.scrollTop = consoleElem.scrollHeight;
            }, 100);
        } else {
            consoleElem.innerText = `Error al cargar logs: ${data.message}`;
        }
    })
    .catch(err => {
        consoleElem.innerText = `Falla crítica al conectar con el servidor: ${err.message}`;
    });
}
