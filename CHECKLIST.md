# Checklist de Implementación y Ejecución

## ✅ Fase 1: Implementación (COMPLETADO)

### Infraestructura Docker
- [x] docker-compose.yml con 5 servicios
- [x] Dockerfile para Python/Spark
- [x] requirements.txt con todas las dependencias
- [x] .env.example con variables de configuración
- [x] .gitignore actualizado para el proyecto
- [x] .dockerignore para optimizar builds

### Código Fuente
- [x] src/productor.py - Productor Kafka completo
- [x] src/procesador_streaming.py - Consumer Spark con estadísticas
- [x] src/entrenamiento_ml.py - Training pipeline ML
- [x] src/utils/inference.py - Inferencia en tiempo real
- [x] src/__init__.py y src/utils/__init__.py

### Scripts de Utilidad
- [x] scripts/setup.sh - Setup automatizado
- [x] scripts/train_model.sh - Entrenar modelo
- [x] scripts/start_consumer.sh - Iniciar consumer
- [x] scripts/start_producer.sh - Iniciar producer
- [x] scripts/stop_services.sh - Detener servicios
- [x] scripts/view_logs.sh - Ver logs
- [x] Todos los scripts con permisos de ejecución

### Configuración
- [x] config/spark/spark-defaults.conf - Config optimizada CPU
- [x] Estructura de directorios completa
- [x] .gitkeep en directorios de datos

### Documentación
- [x] README.md completo (200+ líneas)
- [x] QUICK_START.md para inicio rápido
- [x] PROJECT_SUMMARY.md con resumen técnico
- [x] CHECKLIST.md (este archivo)

### Notebooks
- [x] notebooks/01_analisis_exploratorio.ipynb

---

## 🔄 Fase 2: Ejecución Local (PENDIENTE)

### Pre-requisitos
- [ ] Docker instalado y corriendo
- [ ] Docker Compose instalado
- [ ] Al menos 8 GB RAM disponible
- [ ] Al menos 10 GB espacio en disco
- [ ] Conexión a internet (para descargar Statsbomb)

### Setup Inicial
- [ ] Clonar/tener el repositorio localmente
- [ ] Ejecutar `chmod +x scripts/*.sh`
- [ ] Ejecutar `./scripts/setup.sh`
- [ ] Verificar servicios: `docker-compose ps`
- [ ] Esperar 1-2 minutos para que servicios inicien

### Verificación de Servicios
- [ ] Zookeeper en puerto 2181
- [ ] Kafka en puerto 9092
- [ ] Spark Master en puerto 7077
- [ ] Spark UI accesible en http://localhost:4040 (después de iniciar consumer)
- [ ] pyspark-app container corriendo

### Entrenamiento del Modelo
- [ ] Ejecutar `./scripts/train_model.sh`
- [ ] Esperar ~5-15 minutos (depende del hardware)
- [ ] Verificar modelo creado: `docker exec pyspark-app ls /app/data/models/`
- [ ] Debe existir: modelo.joblib, scaler.joblib, model_metadata.joblib

### Ejecución del Streaming
- [ ] **Terminal 1**: Ejecutar `./scripts/start_consumer.sh`
- [ ] Verificar mensaje "Kafka stream connected"
- [ ] Abrir navegador en http://localhost:4040
- [ ] Verificar Spark UI carga correctamente
- [ ] **Terminal 2**: Ejecutar `./scripts/start_producer.sh`
- [ ] Verificar eventos siendo enviados: `[SENT] Match...`
- [ ] En Terminal 1, verificar estadísticas siendo mostradas
- [ ] Dejar correr por al menos 1 partido completo

### Captura de Datos
- [ ] Tomar screenshots de Spark UI (Jobs, Stages, Executors)
- [ ] Anotar métricas clave en tabla
- [ ] Guardar tiempo total de ejecución
- [ ] Documentar cualquier warning o error

---

## 📊 Fase 3: Comparación de Arquitecturas (PENDIENTE)

### Arquitectura 1: Intel i7 / 32GB

#### Preparación
- [ ] Git clone del repositorio
- [ ] Ejecutar setup completo
- [ ] Entrenar modelo ML
- [ ] Verificar todos los servicios

#### Ejecución
- [ ] Iniciar consumer Spark
- [ ] Iniciar producer Kafka
- [ ] Procesar exactamente 5 partidos
- [ ] No interrumpir durante la ejecución

#### Captura de Métricas
- [ ] Screenshot: Jobs Tab (lista completa)
- [ ] Screenshot: Stages Tab (detalles de stages)
- [ ] Screenshot: Executors Tab (uso de memoria)
- [ ] Screenshot: Streaming Tab (si disponible)
- [ ] Anotar en tabla:
  - [ ] Tiempo total de ejecución
  - [ ] Avg Executor Run Time
  - [ ] Total GC Time
  - [ ] Shuffle Read/Write
  - [ ] Memory Spill
  - [ ] Disk Spill
  - [ ] Scheduler Delay

### Arquitectura 2: Intel i3 / 8GB

#### Preparación
- [ ] Git clone del repositorio
- [ ] Ejecutar setup completo
- [ ] Entrenar modelo ML
- [ ] Verificar todos los servicios

#### Ejecución
- [ ] Iniciar consumer Spark
- [ ] Iniciar producer Kafka
- [ ] Procesar exactamente 5 partidos (mismo dataset)
- [ ] No interrumpir durante la ejecución

#### Captura de Métricas
- [ ] Screenshot: Jobs Tab (lista completa)
- [ ] Screenshot: Stages Tab (detalles de stages)
- [ ] Screenshot: Executors Tab (uso de memoria)
- [ ] Screenshot: Streaming Tab (si disponible)
- [ ] Anotar en tabla:
  - [ ] Tiempo total de ejecución
  - [ ] Avg Executor Run Time
  - [ ] Total GC Time
  - [ ] Shuffle Read/Write
  - [ ] Memory Spill
  - [ ] Disk Spill
  - [ ] Scheduler Delay

---

## 📈 Fase 4: Análisis de Resultados (PENDIENTE)

### Comparación de Métricas
- [ ] Crear tabla comparativa de todas las métricas
- [ ] Calcular diferencias porcentuales
- [ ] Identificar métrica con mayor diferencia
- [ ] Identificar cuellos de botella en cada arquitectura

### Visualizaciones
- [ ] Gráfica de tiempo de ejecución
- [ ] Gráfica de uso de memoria
- [ ] Gráfica de GC Time
- [ ] Gráfica de throughput (eventos/segundo)

### Análisis Técnico
- [ ] Explicar por qué el i7 es más rápido (CPU, memoria)
- [ ] Analizar spill en i3 (si ocurre)
- [ ] Comparar shuffle performance
- [ ] Evaluar escalabilidad (¿qué pasa con 10 partidos?)

### Conclusiones
- [ ] Resumen de hallazgos principales
- [ ] Recomendaciones de hardware para Big Data
- [ ] Lecciones aprendidas sobre Spark tuning
- [ ] Próximos pasos de optimización

---

## 🎯 Fase 5: Entregables Finales (PENDIENTE)

### Repositorio GitHub
- [ ] Repositorio público creado
- [ ] Todo el código subido
- [ ] README.md visible en la página principal
- [ ] .gitignore funcionando (no subir data/)
- [ ] Branch principal limpio y estable

### Documentación
- [ ] README.md completo y actualizado
- [ ] Screenshots incluidos (en docs/ o README)
- [ ] Instrucciones claras de ejecución
- [ ] Sección de troubleshooting completa
- [ ] Resultados de comparación documentados

### Presentación (Opcional)
- [ ] Slides con arquitectura del sistema
- [ ] Diagramas de flujo de datos
- [ ] Screenshots de Spark UI comparativos
- [ ] Gráficas de métricas
- [ ] Conclusiones y aprendizajes

### Reporte Técnico (Opcional)
- [ ] Introducción al problema
- [ ] Descripción de la arquitectura
- [ ] Metodología de comparación
- [ ] Resultados experimentales
- [ ] Análisis de resultados
- [ ] Conclusiones
- [ ] Referencias

---

## 🐛 Troubleshooting Checklist

### Si algo falla:

#### "Container no inicia"
- [ ] Verificar logs: `docker-compose logs [service]`
- [ ] Verificar puertos disponibles: `lsof -i :9092` (ejemplo)
- [ ] Reiniciar: `docker-compose restart [service]`

#### "Kafka connection refused"
- [ ] Esperar 1-2 minutos después de `docker-compose up`
- [ ] Verificar Kafka: `docker-compose logs kafka`
- [ ] Reiniciar Kafka: `docker-compose restart kafka zookeeper`

#### "No module named 'statsbombpy'"
- [ ] Rebuild: `docker-compose build --no-cache`
- [ ] Verificar requirements.txt está montado
- [ ] Reinstalar: `docker exec pyspark-app pip install -r requirements.txt`

#### "Spark UI no carga (404)"
- [ ] Verificar que consumer esté corriendo
- [ ] Verificar puerto 4040: `docker ps | grep 4040`
- [ ] Esperar a que el primer job inicie

#### "Out of memory"
- [ ] Reducir SPARK_WORKER_MEMORY en docker-compose.yml
- [ ] Reducir MATCH_LIMIT (procesar menos partidos)
- [ ] Cerrar otras aplicaciones

#### "Disco lleno"
- [ ] Limpiar Docker: `docker system prune -a --volumes`
- [ ] Limpiar checkpoints: `rm -rf data/checkpoints/*`
- [ ] Limpiar parquet: `rm -rf data/processed/*`

---

## 📋 Tabla de Métricas (Plantilla)

Copiar esta tabla y llenar con valores reales:

| Métrica | i7/32GB | i3/8GB | Diferencia |
|---------|---------|--------|------------|
| **Tiempo Total (s)** | | | |
| **Avg Task Time (s)** | | | |
| **Total GC Time (s)** | | | |
| **Shuffle Read (MB)** | | | |
| **Shuffle Write (MB)** | | | |
| **Memory Spill (MB)** | | | |
| **Disk Spill (MB)** | | | |
| **Scheduler Delay (s)** | | | |
| **Input Rate (rec/s)** | | | |
| **Processing Time (s)** | | | |

---

## ✅ Verificación Final

Antes de considerar el proyecto completo:

### Funcionalidad
- [ ] Producer envía eventos a Kafka
- [ ] Consumer recibe y procesa eventos
- [ ] Estadísticas se calculan correctamente
- [ ] Datos se guardan en Parquet
- [ ] Modelo ML entrena sin errores
- [ ] Todo funciona end-to-end

### Calidad
- [ ] Código documentado con comentarios
- [ ] Sin errores críticos en logs
- [ ] README claro y completo
- [ ] Scripts funcionan correctamente

### Comparación
- [ ] Ejecutado en ambas arquitecturas
- [ ] Métricas capturadas de ambas
- [ ] Resultados documentados
- [ ] Análisis completado

---

**Estado Actual**: ✅ Implementación Completa
**Próximo Paso**: Ejecutar localmente (Fase 2)

---

## 📞 Ayuda Rápida

### Comandos más usados:
```bash
# Iniciar todo
./scripts/setup.sh

# Entrenar modelo
./scripts/train_model.sh

# Ejecutar streaming (2 terminales)
./scripts/start_consumer.sh  # Terminal 1
./scripts/start_producer.sh  # Terminal 2

# Ver logs
./scripts/view_logs.sh

# Detener todo
./scripts/stop_services.sh

# Limpiar y reiniciar
docker-compose down -v
./scripts/setup.sh
```

### URLs importantes:
- Spark UI: http://localhost:4040
- Spark Master: http://localhost:8080

---

**¡Buena suerte con tu proyecto! 🚀**
