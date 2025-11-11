# Proyecto: Arquitectura de Grandes Volúmenes de Datos
## Análisis en Tiempo Real de Datos de Fútbol con Spark Streaming y Kafka

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-7.5.0-black.svg)](https://kafka.apache.org/)

---

## Descripción del Proyecto

Este proyecto implementa un sistema completo de procesamiento de datos en tiempo real utilizando **Apache Spark Structured Streaming** y **Apache Kafka** para analizar eventos de partidos de fútbol de La Liga (Statsbomb).

### Características Principales

- **Simulación de streaming en tiempo real** de eventos de partidos
- **Procesamiento distribuido** con Apache Spark
- **Cálculo de estadísticas en tiempo real**:
  - Posesión del balón por equipo (ventanas de 5 minutos)
  - Expected Goals (xG) promedio
  - Porcentaje de pases completados
- **Machine Learning** para predicción de resultados de partidos
- **Inferencia en tiempo real** integrada al streaming
- **Almacenamiento en formato Parquet** para análisis histórico
- **Comparación de rendimiento** entre diferentes arquitecturas de hardware

---

## Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────┐
│                        DOCKER ENVIRONMENT                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌────────────┐      ┌──────────────┐      ┌───────────────┐   │
│  │ Statsbomb  │ ───> │    Kafka     │ ───> │ Spark Master  │   │
│  │  Producer  │      │   Broker     │      │   + Worker    │   │
│  └────────────┘      └──────────────┘      └───────────────┘   │
│        │                    │                       │           │
│        │                    │                       ▼           │
│        │                    │              ┌──────────────┐     │
│        │                    │              │  Streaming   │     │
│        │                    │              │  Consumer    │     │
│        │                    │              └──────────────┘     │
│        │                    │                       │           │
│        ▼                    ▼                       ▼           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Data Storage (Parquet)                      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              ▼                                  │
│                     ┌──────────────────┐                        │
│                     │   ML Training    │                        │
│                     │   + Inference    │                        │
│                     └──────────────────┘                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Requisitos del Sistema

### Hardware Mínimo
- **CPU**: 2 cores
- **RAM**: 4 GB
- **Disco**: 10 GB libres

### Software Requerido
- Docker Desktop / Docker Engine (20.10+)
- Docker Compose (1.29+)
- Git

---

## Estructura del Proyecto

```
no_cuda/
├── docker-compose.yml          # Orquestación de servicios
├── Dockerfile                  # Imagen Python/Spark
├── requirements.txt            # Dependencias Python
├── .env.example               # Variables de entorno
├── README.md                  # Este archivo
│
├── src/                       # Código fuente
│   ├── productor.py          # Productor Kafka (Statsbomb)
│   ├── procesador_streaming.py  # Consumidor Spark Streaming
│   ├── entrenamiento_ml.py   # Entrenamiento del modelo ML
│   └── utils/                # Utilidades
│       ├── __init__.py
│       └── inference.py      # Inferencia en tiempo real
│
├── data/                      # Datos (gitignored)
│   ├── raw/                  # Datos crudos
│   ├── processed/            # Parquet files
│   └── models/               # Modelos ML entrenados
│
├── notebooks/                 # Jupyter notebooks (opcional)
│
└── config/                    # Configuraciones
    ├── spark/
    └── kafka/
```

---

## Instalación y Configuración

### 1. Clonar el Repositorio

```bash
git clone <repository-url>
cd no_cuda
```

### 2. Configurar Variables de Entorno (Opcional)

```bash
cp .env.example .env
# Editar .env si es necesario
```

### 3. Construir y Levantar el Entorno

```bash
# Construir las imágenes
docker-compose build

# Levantar todos los servicios
docker-compose up -d

# Verificar que todos los servicios estén corriendo
docker-compose ps
```

**Servicios levantados**:
- `zookeeper` → Puerto 2181
- `kafka` → Puerto 9092
- `spark-master` → Puertos 7077, 8080, 4040
- `spark-worker` → Puerto 8081
- `pyspark-app` → Contenedor de aplicación

### 4. Verificar el Estado de los Servicios

```bash
# Ver logs de todos los servicios
docker-compose logs -f

# Ver logs de un servicio específico
docker-compose logs -f kafka
docker-compose logs -f spark-master
```

---

## Uso del Sistema

### Paso 1: Entrenar el Modelo de Machine Learning

**Importante**: Ejecutar esto PRIMERO antes del streaming.

```bash
# Acceder al contenedor de la aplicación
docker exec -it pyspark-app bash

# Dentro del contenedor, ejecutar el entrenamiento
python src/entrenamiento_ml.py
```

Este script:
1. Descarga datos históricos de Statsbomb (La Liga)
2. Extrae características de los primeros tiempos
3. Entrena un modelo Random Forest
4. Guarda el modelo en `/app/data/models/modelo.joblib`

**Tiempo estimado**: 5-15 minutos (dependiendo del hardware)

---

### Paso 2: Iniciar el Streaming Consumer (Spark)

En **otra terminal** (manteniendo la primera abierta):

```bash
# Acceder al contenedor
docker exec -it pyspark-app bash

# Ejecutar el consumidor Spark Streaming
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  src/procesador_streaming.py
```

Este script:
1. Se conecta a Kafka
2. Espera eventos en el topic `statsbomb-eventos`
3. Calcula estadísticas en tiempo real
4. Guarda datos en Parquet
5. Muestra estadísticas en consola

**Acceder a la Spark UI**:
- Abrir navegador en: **http://localhost:4040**
- Esta interfaz muestra métricas en tiempo real

---

### Paso 3: Ejecutar el Productor de Kafka

En **una tercera terminal**:

```bash
# Acceder al contenedor
docker exec -it pyspark-app bash

# Ejecutar el productor
python src/productor.py
```

Este script:
1. Descarga partidos de La Liga de Statsbomb
2. Envía eventos a Kafka uno por uno
3. Simula streaming en tiempo real (0.5s entre eventos)

**Verás en consola**:
```
[SENT] Match 12345 | 15:23 | Pass | Partition: 0, Offset: 42
[SENT] Match 12345 | 15:25 | Shot | Partition: 0, Offset: 43
```

---

### Paso 4: Observar Resultados

#### En la terminal del Consumer (Paso 2):

Verás tablas actualizándose con estadísticas como:

```
+-------------------+-------------------+-----------+--------------------+
|window_start       |window_end         |team_name  |possession_percentage|
+-------------------+-------------------+-----------+--------------------+
|2025-11-11 10:00:00|2025-11-11 10:05:00|Barcelona  |62.5                |
|2025-11-11 10:00:00|2025-11-11 10:05:00|Real Madrid|37.5                |
+-------------------+-------------------+-----------+--------------------+
```

#### En la Spark UI (http://localhost:4040):

- **Jobs**: Ver jobs ejecutándose
- **Stages**: Detalles de cada stage
- **Streaming**: Batch duration, processing time
- **Environment**: Configuración de Spark
- **Executors**: Uso de recursos

---

## Comparación de Rendimiento entre Arquitecturas

Este proyecto permite comparar el rendimiento del mismo código en diferentes máquinas.

### Arquitecturas a Comparar

| Arquitectura | Procesador | RAM  | Tipo |
|--------------|-----------|------|------|
| **Arquitectura 1** | Intel i7 (11va gen) | 32 GB | Alto Rendimiento |
| **Arquitectura 2** | Intel i3 | 8 GB | Bajo Rendimiento |

### Métricas a Capturar

#### 1. Desde la Spark UI (http://localhost:4040)

**Jobs Tab**:
- [ ] Tiempo total de ejecución de cada job
- [ ] Número de stages por job
- [ ] Número de tasks completadas

**Stages Tab**:
- [ ] Duration (duración total del stage)
- [ ] Shuffle Read Size / Records
- [ ] Shuffle Write Size / Records
- [ ] Input Size / Records
- [ ] Output Size / Records

**Tasks Tab** (dentro de cada Stage):
- [ ] **Scheduler Delay**: Tiempo esperando a ser ejecutada
- [ ] **Task Deserialization Time**
- [ ] **Executor Run Time**: Tiempo de ejecución real
- [ ] **Result Serialization Time**
- [ ] **GC Time**: Tiempo en Garbage Collection
- [ ] **Shuffle Read Time**
- [ ] **Shuffle Write Time**

**Executors Tab**:
- [ ] Memory Used / Available
- [ ] Disk Used (Spill Memory, Spill Disk)
- [ ] Task Time
- [ ] GC Time
- [ ] Failed Tasks

**Streaming Tab** (si está disponible):
- [ ] Input Rate (eventos/segundo)
- [ ] Processing Time por batch
- [ ] Scheduling Delay
- [ ] Total Delay

#### 2. Desde el Sistema Operativo

```bash
# CPU usage
docker stats pyspark-app spark-master spark-worker

# Disk I/O
docker exec -it pyspark-app df -h
```

#### 3. Métricas del Streaming

- Throughput (eventos procesados por segundo)
- Latencia (tiempo desde ingesta hasta procesamiento)
- Backpressure (si el consumer se atrasa)

---

### Cómo Realizar la Comparación

1. **Ejecutar el mismo flujo completo en ambas máquinas**:
   - Mismo número de partidos
   - Mismo delay entre eventos
   - Misma configuración de Spark

2. **Capturar screenshots de la Spark UI**:
   - Jobs Tab completa
   - Stage details de stages críticos
   - Task metrics
   - Executors Tab

3. **Anotar métricas en una tabla**:

| Métrica | Arquitectura 1 (i7/32GB) | Arquitectura 2 (i3/8GB) |
|---------|--------------------------|-------------------------|
| Tiempo total de ejecución | | |
| Avg Task Run Time | | |
| Total Shuffle Read | | |
| Total Shuffle Write | | |
| GC Time Total | | |
| Memory Spill | | |
| Disk Spill | | |
| Scheduler Delay | | |

4. **Analizar diferencias**:
   - ¿Dónde está el cuello de botella en cada arquitectura?
   - ¿El i3 tiene más spill a disco?
   - ¿El GC Time es significativamente mayor en el i3?
   - ¿Hay diferencias en shuffle performance?

---

## Configuración de Spark para Comparación Justa

Para asegurar una comparación válida, ambas máquinas deben usar la misma configuración:

### En `docker-compose.yml` (sección spark-worker):

```yaml
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=2G
    - SPARK_WORKER_CORES=2
```

**Mantener esto igual en ambas máquinas** para una comparación justa, o ajustarlo proporcionalmente si quieres probar diferentes configuraciones.

---

## Detalles de Implementación

### 1. Productor de Kafka (`src/productor.py`)

**Funcionalidad**:
- Usa `statsbombpy` para descargar datos
- Convierte eventos a JSON
- Envía a topic `statsbomb-eventos`
- Delay configurable entre eventos

**Configuración**:
```python
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=statsbomb-eventos
EVENT_DELAY=0.5  # segundos
MATCH_LIMIT=5    # número de partidos
```

---

### 2. Consumidor Spark Streaming (`src/procesador_streaming.py`)

**Funcionalidad**:
- Lee de Kafka usando Spark Structured Streaming
- Calcula estadísticas usando operaciones de ventana (window)
- Guarda datos crudos en Parquet
- Muestra estadísticas en consola

**Estadísticas Calculadas**:

1. **Posesión**:
   ```python
   window(col("event_timestamp"), "5 minutes")
   count(*) by team
   ```

2. **xG (Expected Goals)**:
   ```python
   filter(type == 'Shot')
   avg(shot.statsbomb_xg)
   ```

3. **Pases Completados**:
   ```python
   filter(type == 'Pass')
   count where pass.outcome is NULL
   ```

---

### 3. Modelo de Machine Learning (`src/entrenamiento_ml.py`)

**Features Extraídas** (del primer tiempo):
- Posesión del balón
- Número de tiros
- xG total
- Pases totales y completados
- Tasa de éxito en pases
- Duelos y tackles

**Target (Resultado)**:
- `home_win`: Victoria local
- `away_win`: Victoria visitante
- `draw`: Empate

**Modelo**:
- Random Forest Classifier (100 árboles, max_depth=10)
- Train/Test split: 80/20
- Cross-validation de 5 folds

---

## Troubleshooting

### Problema: Kafka no se conecta

```bash
# Verificar que Kafka esté corriendo
docker-compose logs kafka

# Reiniciar Kafka
docker-compose restart kafka
```

### Problema: Spark no encuentra el Master

```bash
# Verificar que Spark Master esté levantado
docker-compose logs spark-master

# Verificar la URL del master
docker exec -it pyspark-app env | grep SPARK_MASTER
```

### Problema: Errores de memoria en Spark

Ajustar en `docker-compose.yml`:

```yaml
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=4G  # Aumentar si es posible
```

### Problema: Producer no encuentra datos de Statsbomb

```bash
# Verificar conectividad a internet desde el contenedor
docker exec -it pyspark-app ping -c 3 google.com

# Verificar instalación de statsbombpy
docker exec -it pyspark-app pip show statsbombpy
```

### Problema: "No space left on device"

```bash
# Limpiar volúmenes de Docker
docker system prune -a --volumes

# Liberar espacio en datos
docker exec -it pyspark-app rm -rf /app/data/checkpoints/*
```

---

## Limpieza y Apagado

```bash
# Detener todos los servicios
docker-compose down

# Detener y eliminar volúmenes (CUIDADO: borra datos)
docker-compose down -v

# Eliminar imágenes
docker-compose down --rmi all
```

---

## Extensiones Futuras

- [ ] Dashboard en tiempo real con Grafana
- [ ] Más modelos de ML (XGBoost, Neural Networks)
- [ ] Predicción de eventos individuales (goles, tarjetas)
- [ ] Integración con bases de datos (PostgreSQL, MongoDB)
- [ ] API REST para consultas
- [ ] Alertas en tiempo real (webhooks, email)

---

## Referencias

- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Statsbomb Open Data](https://github.com/statsbomb/open-data)
- [Statsbombpy Library](https://github.com/statsbomb/statsbombpy)

---

## Autores

Proyecto desarrollado para la asignatura **Arquitectura de Grandes Volúmenes de Datos**.

---

## Licencia

Este proyecto es de uso educativo. Los datos de Statsbomb están sujetos a su propia licencia.

---

**¡Buena suerte con tu proyecto! 🚀⚽**
