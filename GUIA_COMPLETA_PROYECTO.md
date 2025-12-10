# Guía Completa del Proyecto: Sistema de Análisis de Fútbol en Tiempo Real

## 📋 Tabla de Contenidos
1. [¿Qué Hace Este Proyecto?](#qué-hace-este-proyecto)
2. [Arquitectura General](#arquitectura-general)
3. [Pipeline de Datos Completo](#pipeline-de-datos-completo)
4. [Componentes Principales](#componentes-principales)
5. [Stack Tecnológico](#stack-tecnológico)
6. [Estructura del Proyecto](#estructura-del-proyecto)
7. [Flujo de Ejecución](#flujo-de-ejecución)
8. [Detalles Técnicos Importantes](#detalles-técnicos-importantes)
9. [Monitoreo y Debugging](#monitoreo-y-debugging)
10. [Casos de Uso](#casos-de-uso)

---

## 🎯 ¿Qué Hace Este Proyecto?

Este es un **sistema de análisis de eventos deportivos en tiempo real** que simula un pipeline completo de Big Data para procesar datos de partidos de fútbol.

### Objetivos Principales:
- ✅ Demostrar procesamiento de streaming en tiempo real con Spark
- ✅ Implementar un pipeline completo de ingesta → procesamiento → ML → visualización
- ✅ Comparar rendimiento entre diferentes arquitecturas de hardware
- ✅ Mostrar integración de tecnologías Big Data modernas (Kafka + Spark + ML)

### ¿Qué Hace en Concreto?
1. **Obtiene datos reales** de partidos de La Liga 2020/2021 desde la API de Statsbomb
2. **Simula streaming** enviando eventos a Kafka como si ocurrieran en tiempo real
3. **Procesa eventos** con Spark Structured Streaming calculando estadísticas en vivo
4. **Predice resultados** usando modelos de Machine Learning entrenados con datos históricos
5. **Almacena datos** en formato Parquet para análisis posteriores

---

## 🏗️ Arquitectura General

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DOCKER COMPOSE                              │
│  ┌──────────┐  ┌────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐  │
│  │Zookeeper │→ │ Kafka  │← │  Spark   │← │  Spark   │← │ Spark  │  │
│  │          │  │ Broker │  │  Master  │  │  Worker  │  │  App   │  │
│  │ :2181    │  │ :9092  │  │:7077/8080│  │  2GB/2C  │  │ :4040  │  │
│  └──────────┘  └────────┘  └──────────┘  └──────────┘  └────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                                    ↑
                              Volumes montados
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│  /src/           /data/              /notebooks/                     │
│  - productor.py  - raw_events/       - 01_analisis.ipynb            │
│  - procesador    - checkpoint/                                       │
│  - entrena ML    - model/                                            │
└─────────────────────────────────────────────────────────────────────┘
```

### Flujo de Datos:
```
API Statsbomb → Productor → Kafka → Spark Streaming → Parquet
                              ↓                          ↓
                        Cola de Mensajes        Almacenamiento
                              ↓                          ↓
                     Spark Consumer  ←  Entrenamiento ML
                              ↓                          ↓
                    Procesamiento Batch        Modelo RF
                              ↓                          ↓
                     Estadísticas Live → Predicciones
```

---

## 🔄 Pipeline de Datos Completo

### Fase 1: INGESTA
**Archivo**: [src/productor.py](src/productor.py)

```python
API Statsbomb (REST)
    ↓
Obtener matches La Liga 2020/21
    ↓
Por cada match, obtener eventos (~3000 eventos/partido)
    ↓
Serializar a JSON
    ↓
Publicar a Kafka topic: "statsbomb-eventos"
    ↓
Rate: ~100 eventos/segundo (configurable con delay de 0.05s)
```

**Datos que envía**:
- ID del evento, timestamp, minuto, segundo
- Tipo de evento (Pass, Shot, Duel, etc.)
- Equipo, jugador, posición
- Coordenadas en el campo
- Métricas especiales (xG para disparos, resultados de pases)

### Fase 2: MENSAJERÍA
**Componente**: Kafka Broker (Docker)

```
Topic: statsbomb-eventos
├─ Particiones: 1 (por defecto)
├─ Formato: JSON strings
├─ Retención: Default (hasta que se consuma)
└─ Serialización: UTF-8
```

**¿Por qué Kafka?**
- Buffer entre productor y consumidor (desacoplamiento)
- Tolerancia a fallos (reenvío automático)
- Escalabilidad (múltiples consumidores posibles)
- Replay de datos (pueden re-procesarse eventos)

### Fase 3: PROCESAMIENTO
**Archivo**: [src/procesador_streaming.py](src/procesador_streaming.py)

```python
Spark Structured Streaming
    ↓
Leer desde Kafka (micro-batches de 10 segundos)
    ↓
Parsear JSON con schema definido (46 campos)
    ↓
Procesamiento por batch:
    ├─ Agrupar por team + match_id
    ├─ Calcular métricas:
    │   ├─ Posesión (count de eventos)
    │   ├─ xG promedio (expected goals)
    │   └─ Tasa de pases completos
    ↓
Escribir a Parquet (append mode)
    ↓
Si existe modelo ML → Inferencia
    ↓
Mostrar estadísticas en consola
```

**Ventana de procesamiento**:
- Trigger: 10 segundos
- Checkpoints: Cada micro-batch
- Watermark: 5 minutos (para eventos tardíos)

### Fase 4: ALMACENAMIENTO
**Ubicación**: `/app/data/raw_events/*.parquet`

```
Formato Parquet (columnar)
├─ Compresión: Snappy (por defecto)
├─ Schema enforcement: Sí
├─ Particionamiento: Por fecha de escritura
└─ Modo: Append (acumula eventos)
```

**Ventajas de Parquet**:
- 🚀 Compresión eficiente (~10x vs CSV)
- 🎯 Lectura columnar (solo lee columnas necesarias)
- 📊 Compatible con Spark, Pandas, Arrow
- 🔒 Schema integrado (self-documenting)

### Fase 5: MACHINE LEARNING

#### Entrenamiento
**Archivo**: [src/entrenamiento_ml.py](src/entrenamiento_ml.py)

```python
Leer Parquet files
    ↓
Agrupar eventos por (match_id, team)
    ↓
Feature Engineering:
    ├─ possession_count: Total eventos del equipo
    ├─ xg_avg: Promedio de Expected Goals
    └─ pass_completion_rate: % pases completos
    ↓
Crear labels (SINTÉTICAS para demo):
    └─ label = 1 si xg_avg > 0.1 else 0
    ↓
Train Random Forest Classifier:
    ├─ 10 árboles
    ├─ max_depth = 5
    └─ random_state = 42
    ↓
Guardar modelo:
    ├─ Formato Spark MLlib: /app/data/model/
    └─ Formato Joblib: /app/data/models/match_predictor.pkl
```

**Nota Importante**: Las labels son sintéticas (basadas en xG) para propósitos demostrativos. En producción real, usarías resultados reales de partidos.

#### Inferencia en Tiempo Real
**Archivo**: [src/utils/inference.py](src/utils/inference.py)

```python
Clase: MatchPredictor
    ├─ load_model(): Carga modelo joblib
    ├─ extract_features_from_aggregates():
    │   └─ Convierte estadísticas de Spark a features
    ├─ predict(): Genera predicciones con probabilidades
    └─ format_prediction(): Pretty-print de resultados
```

**Integración con Streaming**:
```python
# En procesador_streaming.py
if os.path.exists(MODEL_PATH):
    predictor = MatchPredictor(MODEL_PATH)
    prediction = predictor.predict(aggregated_df)
    # Muestra: "Equipo X tiene 73% probabilidad de ganar"
```

### Fase 6: VISUALIZACIÓN

**Salidas del Sistema**:

1. **Consola (STDOUT)**:
```
=== Estadísticas de Equipo ===
Equipo: Barcelona
├─ Posesión: 450 eventos
├─ xG Promedio: 0.087
└─ Tasa de Pases: 0.85 (85%)

Predicción: Victoria (probabilidad: 0.73)
```

2. **Spark UI** (`http://localhost:4040`):
   - Jobs ejecutados
   - Stages y tasks
   - Tiempos de ejecución
   - Métricas de memoria
   - DAG visualization

3. **Archivos Parquet** (para análisis posterior):
   - Jupyter notebooks
   - Consultas SQL con Spark
   - Export a Pandas/CSV

---

## 🔧 Componentes Principales

### 1. Productor de Datos
**Archivo**: [src/productor.py](src/productor.py:1-80)

```python
from statsbombpy import sb
from kafka import KafkaProducer

# Obtiene competiciones y matches
competitions = sb.competitions()
la_liga = competitions[...filter...]
matches = sb.matches(competition_id, season_id)

# Por cada partido
for match in matches:
    events = sb.events(match_id)

    # Envía cada evento a Kafka
    for event in events:
        producer.send('statsbomb-eventos',
                     value=event.to_json())
        time.sleep(0.05)  # Simula real-time
```

**Configuración Kafka**:
```python
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
```

### 2. Procesador Streaming
**Archivo**: [src/procesador_streaming.py](src/procesador_streaming.py:1-134)

**Schema de Eventos** (46 campos):
```python
schema = StructType([
    StructField("id", StringType(), True),
    StructField("index", IntegerType(), True),
    StructField("period", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("minute", IntegerType(), True),
    StructField("second", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("possession", IntegerType(), True),
    StructField("possession_team", StringType(), True),
    StructField("team", StringType(), True),
    StructField("player", StringType(), True),
    StructField("position", StringType(), True),
    StructField("location", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("under_pressure", BooleanType(), True),
    StructField("match_id", IntegerType(), True),
    # ... más campos
])
```

**Función de Procesamiento**:
```python
def process_batch(batch_df, batch_id):
    # 1. Agregar por equipo
    aggregated = batch_df.groupBy("team", "match_id").agg(
        count("*").alias("possession_count"),
        avg("shot_statsbomb_xg").alias("xg_avg"),
        # más agregaciones...
    )

    # 2. Guardar raw events
    batch_df.write.mode("append").parquet(DATA_PATH)

    # 3. Inferencia ML (si hay modelo)
    if predictor:
        predictions = predictor.predict(aggregated)

    # 4. Mostrar estadísticas
    aggregated.show(truncate=False)
```

**Configuración Streaming**:
```python
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "statsbomb-eventos") \
    .option("startingOffsets", "earliest") \
    .load()

query = parsed_df \
    .writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()
```

### 3. Módulo de ML
**Archivo**: [src/entrenamiento_ml.py](src/entrenamiento_ml.py:1-64)

**Pipeline de Entrenamiento**:
```python
# 1. Leer datos históricos
df = spark.read.parquet("/app/data/raw_events/")

# 2. Agrupar por partido y equipo
features_df = df.groupBy("match_id", "team").agg(
    count("*").alias("possession_count"),
    avg("shot_statsbomb_xg").alias("xg_avg"),
    (sum(when(col("pass_outcome") == "Complete", 1).otherwise(0))
     / count("*")).alias("pass_completion_rate")
)

# 3. Crear labels (SINTÉTICAS)
features_df = features_df.withColumn(
    "label",
    when(col("xg_avg") > 0.1, 1).otherwise(0)
)

# 4. Feature vector
assembler = VectorAssembler(
    inputCols=["possession_count", "xg_avg", "pass_completion_rate"],
    outputCol="features"
)

# 5. Entrenar Random Forest
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=10,
    maxDepth=5
)

pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(features_df)

# 6. Guardar
model.save("/app/data/model/")
```

### 4. Sistema de Inferencia
**Archivo**: [src/utils/inference.py](src/utils/inference.py:1-261)

**Clase Principal**:
```python
class MatchPredictor:
    def __init__(self, model_path: str):
        self.model = joblib.load(model_path)
        self.feature_names = [
            'possession_count',
            'xg_avg',
            'pass_completion_rate'
        ]

    def extract_features_from_aggregates(self, agg_df):
        """Convierte Spark DF a feature matrix"""
        # Maneja valores nulos
        # Alinea columnas con training
        # Retorna numpy array

    def predict(self, agg_df):
        """Genera predicciones con probabilidades"""
        features = self.extract_features_from_aggregates(agg_df)
        predictions = self.model.predict(features)
        probabilities = self.model.predict_proba(features)
        return predictions, probabilities

    def format_prediction(self, team_name, prediction, probability):
        """Pretty-print de resultados"""
        outcome = "Victoria" if prediction == 1 else "Derrota"
        confidence = probability[prediction] * 100
        return f"{team_name}: {outcome} ({confidence:.1f}%)"
```

---

## 🛠️ Stack Tecnológico

### Big Data Framework
| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| **Apache Spark** | 3.5.0 | Motor de procesamiento distribuido |
| **Spark Structured Streaming** | 3.5.0 | Procesamiento de streams |
| **Apache Kafka** | 7.4.0 | Message broker / Event streaming |
| **Zookeeper** | 7.4.0 | Coordinación de Kafka |

### Machine Learning
| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| **Spark MLlib** | 3.5.0 | Entrenamiento distribuido |
| **scikit-learn** | 1.3.2 | Random Forest Classifier |
| **joblib** | 1.3.2 | Serialización de modelos |

### Python Stack
| Librería | Versión | Propósito |
|----------|---------|-----------|
| **PySpark** | 3.5.0 | API Python para Spark |
| **kafka-python** | 2.0.2 | Cliente Kafka |
| **statsbombpy** | 1.13.0 | API de datos de fútbol |
| **pandas** | 2.1.4 | Manipulación de datos |
| **numpy** | 1.26.2 | Computación numérica |
| **pyarrow** | 14.0.1 | Soporte Parquet |

### Infraestructura
| Tecnología | Propósito |
|------------|-----------|
| **Docker** | Containerización |
| **Docker Compose** | Orquestación multi-container |
| **Bash** | Scripts de automatización |

### Formatos de Datos
- **JSON**: Serialización en Kafka
- **Parquet**: Almacenamiento columnar
- **Joblib**: Persistencia de modelos ML

---

## 📁 Estructura del Proyecto

```
/home/schafler/ITAM/Arqui/noCUDA/
│
├── 📂 src/                          # CÓDIGO FUENTE
│   ├── productor.py                 # [ENTRADA] Statsbomb → Kafka
│   ├── procesador_streaming.py     # [CORE] Kafka → Spark → Parquet
│   ├── entrenamiento_ml.py         # [ML] Entrena Random Forest
│   │
│   ├── 📂 utils/
│   │   └── inference.py            # Predicciones en tiempo real
│   │
│   ├── check_kafka.py              # Test conectividad Kafka
│   ├── inspect_data.py             # Inspector de Parquet
│   ├── inspect_sb_data.py          # Inspector de Statsbomb API
│   └── run_pipeline.sh             # Runner automático
│
├── 📂 scripts/                      # AUTOMATIZACIÓN
│   ├── setup.sh                    # ⚙️ Inicialización completa
│   ├── start_producer.sh           # ▶️ Iniciar productor
│   ├── start_consumer.sh           # ▶️ Iniciar consumidor Spark
│   ├── train_model.sh              # 🤖 Entrenar modelo ML
│   ├── stop_services.sh            # ⏹️ Detener servicios
│   └── view_logs.sh                # 📋 Ver logs
│
├── 📂 data/                         # DATOS (gitignored)
│   ├── raw_events/                 # Parquet: eventos raw
│   ├── checkpoint/                 # Checkpoints Spark
│   ├── checkpoints/                # Backup checkpoints
│   ├── model/                      # Modelo Spark MLlib
│   ├── models/                     # Modelos joblib
│   ├── processed/                  # Datos procesados
│   └── raw/                        # Cache raw
│
├── 📂 config/                       # CONFIGURACIÓN
│   └── spark/
│       └── spark-defaults.conf     # Parámetros Spark
│
├── 📂 docs/                         # DOCUMENTACIÓN
│   ├── PROJECT_SUMMARY.md          # Resumen completo
│   └── QUICK_START.md              # Guía rápida
│
├── 📂 notebooks/                    # ANÁLISIS
│   └── 01_analisis_exploratorio.ipynb
│
├── docker-compose.yml              # 🐳 Orquestación servicios
├── Dockerfile                      # 🐳 Container aplicación
├── requirements.txt                # 📦 Dependencias Python
├── .env.example                    # 🔧 Variables de entorno
├── .gitignore                      # 🚫 Exclusiones Git
├── README.md                       # 📖 Documentación principal
└── GUIA_COMPLETA_PROYECTO.md       # 📚 Esta guía
```

### Archivos Críticos por Categoría

#### 🎯 Aplicación Core
1. **[src/productor.py](src/productor.py)** (80 líneas)
   - Entry point de ingesta
   - Configura delay entre eventos
   - Maneja errores de conexión

2. **[src/procesador_streaming.py](src/procesador_streaming.py)** (134 líneas)
   - Main streaming app
   - Define schema completo
   - Integra ML inference
   - Función `process_batch()` crítica

3. **[src/entrenamiento_ml.py](src/entrenamiento_ml.py)** (64 líneas)
   - Pipeline ML completo
   - Feature engineering
   - Guardado dual (MLlib + joblib)

#### 🐳 Infraestructura
1. **[docker-compose.yml](docker-compose.yml)** (63 líneas)
   ```yaml
   services:
     zookeeper:     # Coordinación Kafka
     kafka:         # Message broker
     spark-master:  # Coordinador Spark
     spark-worker:  # Ejecutor (2GB, 2 cores)
     spark-app:     # Container aplicación
   ```

2. **[Dockerfile](Dockerfile)** (24 líneas)
   - Base: `python:3.10-slim`
   - Instala Java 11 (requerido por Spark)
   - Expone puerto 4040 (Spark UI)

#### ⚙️ Configuración
1. **[.env.example](.env.example)** (22 líneas)
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=kafka:29092
   SPARK_MASTER=spark://spark-master:7077
   DATA_PATH=/app/data/raw_events
   TRIGGER_TIME=10 seconds
   PRODUCER_DELAY=0.5
   ```

2. **[config/spark/spark-defaults.conf](config/spark/spark-defaults.conf)** (46 líneas)
   ```
   spark.driver.memory=1g
   spark.executor.memory=2g
   spark.sql.shuffle.partitions=4
   spark.sql.adaptive.enabled=true
   spark.streaming.backpressure.enabled=true
   ```

#### 🤖 Automatización
1. **[scripts/setup.sh](scripts/setup.sh)** - Setup inicial completo
2. **[scripts/start_consumer.sh](scripts/start_consumer.sh)** - Lanza Spark con Kafka package
3. **[scripts/train_model.sh](scripts/train_model.sh)** - Entrenamiento ML

---

## ⚡ Flujo de Ejecución

### Ejecución Normal (Paso a Paso)

#### 1️⃣ PREPARACIÓN INICIAL
```bash
# Clonar repo (si aplica)
cd /home/schafler/ITAM/Arqui/noCUDA

# Ejecutar setup
./scripts/setup.sh
```

**¿Qué hace `setup.sh`?**
1. ✅ Verifica Docker y Docker Compose instalados
2. 📁 Crea directorios en `/data/`:
   - `raw_events/`, `checkpoint/`, `model/`, etc.
3. 📄 Copia `.env.example` → `.env`
4. 🐳 `docker-compose build` (construye imágenes)
5. ▶️ `docker-compose up -d` (inicia 5 servicios)
6. ⏳ Espera 30s para que servicios estén listos

**Resultado esperado**:
```
✅ Docker installed
✅ Docker Compose installed
✅ Data directories created
✅ Environment file created
✅ Services started successfully

Next steps:
  1. docker exec -it spark-app bash
  2. ./scripts/train_model.sh  # Optional
  3. ./scripts/start_consumer.sh &
  4. ./scripts/start_producer.sh
```

#### 2️⃣ VERIFICACIÓN DE SERVICIOS
```bash
# Ver servicios corriendo
docker-compose ps

# Deberías ver:
# zookeeper      Up      2181/tcp
# kafka          Up      9092/tcp, 29092/tcp
# spark-master   Up      7077/tcp, 8080/tcp
# spark-worker   Up      8081/tcp
# spark-app      Up      4040/tcp

# Verificar logs
./scripts/view_logs.sh
```

#### 3️⃣ ENTRENAMIENTO ML (Opcional pero recomendado)
```bash
# Entrar al container
docker exec -it spark-app bash

# Entrenar modelo
./scripts/train_model.sh
```

**¿Qué hace `train_model.sh`?**
```bash
spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=2g \
  src/entrenamiento_ml.py
```

**Salida esperada**:
```
Reading Parquet data from /app/data/raw_events...
Found 12,345 events across 10 matches
Grouping by match_id and team...
Creating features: possession_count, xg_avg, pass_completion_rate
Creating synthetic labels (xG > 0.1 → Win)
Training Random Forest with 10 trees...
Model accuracy: 0.87
Saving model to /app/data/model/
✅ Model saved successfully!
```

**Nota**: Si no hay datos previos, este paso fallará. En ese caso, primero ejecuta el productor/consumidor para generar datos.

#### 4️⃣ INICIAR CONSUMIDOR (Terminal 1)
```bash
# Dentro del container
./scripts/start_consumer.sh
```

**¿Qué hace?**
```bash
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  src/procesador_streaming.py
```

**Salida esperada**:
```
Starting Spark Structured Streaming consumer...
Connecting to Kafka at kafka:29092
Subscribing to topic: statsbomb-eventos
Checkpoint location: /app/data/checkpoint
Starting from: earliest offsets
Waiting for data...
```

**Estado**: El consumidor queda esperando eventos de Kafka.

#### 5️⃣ INICIAR PRODUCTOR (Terminal 2)
```bash
# Abrir nueva terminal
docker exec -it spark-app bash

# Iniciar productor
./scripts/start_producer.sh
```

**¿Qué hace?**
```bash
python src/productor.py
```

**Salida esperada**:
```
Fetching La Liga 2020/2021 competitions...
Found competition ID: 11
Fetching matches for season 90...
Found 380 matches

Processing match 1/380: Barcelona vs Real Madrid
Fetching events for match ID: 3788741...
Found 3,127 events

Publishing to Kafka topic: statsbomb-eventos
Event 1/3127: Pass by Lionel Messi at 00:00:12 ✅
Event 2/3127: Duel by Sergio Ramos at 00:00:15 ✅
Event 3/3127: Shot by Karim Benzema at 00:00:23 ✅
...
[============================] 100% (3127/3127)

Match 1 complete! Published 3,127 events in 156.35 seconds
Rate: ~20 events/second

Processing match 2/380...
```

#### 6️⃣ OBSERVAR PROCESAMIENTO (Terminal 1)
En la terminal del consumidor, verás:

```
Batch 0 received at 2025-12-02 15:30:10
Processing 200 events...

=== Team Statistics ===
+--------------------+----------+-----------------+-------+--------------------+
|team                |match_id  |possession_count |xg_avg |pass_completion_rate|
+--------------------+----------+-----------------+-------+--------------------+
|Barcelona           |3788741   |105              |0.087  |0.85                |
|Real Madrid         |3788741   |95               |0.092  |0.82                |
+--------------------+----------+-----------------+-------+--------------------+

=== ML Predictions ===
Barcelona: Victoria (73.2% probabilidad)
Real Madrid: Derrota (26.8% probabilidad)

Events written to: /app/data/raw_events/
Checkpoint saved: /app/data/checkpoint/

Waiting for next batch...
---

Batch 1 received at 2025-12-02 15:30:20
Processing 200 events...
...
```

#### 7️⃣ MONITOREO (Terminal 3 - Opcional)
```bash
# Spark UI
open http://localhost:4040

# Kafka Manager (si lo instalaste)
open http://localhost:9000

# Ver logs en tiempo real
docker logs -f spark-app
```

---

### Ejecución Automática (Para Pruebas Rápidas)

Si prefieres una ejecución automatizada:

```bash
# Opción 1: Pipeline completo automatizado
docker exec -it spark-app bash
python src/run_pipeline.sh

# Opción 2: Con límite de matches
MATCH_LIMIT=5 python src/run_pipeline.sh
```

**¿Qué hace `run_pipeline.sh`?**
1. Inicia consumidor en background
2. Espera 10s
3. Inicia productor
4. Monitorea progreso
5. Al finalizar, muestra resumen de métricas

---

## 🔍 Detalles Técnicos Importantes

### 1. Schema de Eventos (46 Campos)

```python
# Campos de identificación
id: String              # UUID del evento
index: Integer          # Número secuencial
match_id: Integer       # ID del partido

# Campos temporales
period: Integer         # 1 o 2 (mitad)
timestamp: String       # HH:MM:SS.mmm
minute: Integer         # Minuto del partido
second: Integer         # Segundo dentro del minuto

# Campos de contexto
type: String            # Pass, Shot, Duel, etc.
possession: Integer     # Número de posesión
possession_team: String # Equipo con posesión

# Entidades
team: String            # Nombre del equipo
player: String          # Nombre del jugador
position: String        # Posición en el campo

# Espaciales
location: String        # [x, y] en el campo
end_location: String    # [x, y] destino

# Métricas
duration: Double        # Duración en segundos
under_pressure: Boolean # Si estaba presionado

# Pass específicos
pass_length: Double
pass_angle: Double
pass_height: String     # Ground/Low/High
pass_end_location: String
pass_recipient: String
pass_outcome: String    # Complete/Incomplete/Out

# Shot específicos
shot_statsbomb_xg: Double  # Expected Goals (0-1)
shot_outcome: String       # Goal/Saved/Off Target
shot_technique: String
shot_body_part: String
shot_end_location: String

# Duel específicos
duel_type: String
duel_outcome: String

# Otros eventos
interception_outcome: String
clearance_body_part: String
goalkeeper_type: String
# ... más campos específicos por tipo
```

### 2. Configuración de Spark

**Driver** (Coordina trabajo):
- Memory: 1GB
- Cores: 1
- Ejecuta en `spark-app` container

**Executor** (Hace el trabajo):
- Memory: 2GB
- Cores: 2
- Ejecuta en `spark-worker` container

**Optimizaciones Aplicadas**:
```conf
# Serialización eficiente
spark.serializer=org.apache.spark.serializer.KryoSerializer

# Shuffle optimizado
spark.sql.shuffle.partitions=4
spark.shuffle.compress=true
spark.shuffle.file.buffer=64k

# Adaptive Query Execution
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true

# Backpressure (previene overwhelm)
spark.streaming.backpressure.enabled=true
spark.streaming.kafka.maxRatePerPartition=100

# Checkpointing
spark.streaming.stopGracefullyOnShutdown=true
```

### 3. Configuración de Kafka

```yaml
# docker-compose.yml
KAFKA_BROKER_ID: 1
KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_INTERNAL://kafka:29092
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

**Puertos**:
- `9092`: Para clientes externos (host)
- `29092`: Para clientes internos (Docker network)

### 4. Feature Engineering

**Features Calculadas**:
```python
# 1. Posesión (proxy de control del juego)
possession_count = count(eventos_del_equipo)

# 2. Calidad ofensiva (expected goals)
xg_avg = avg(shot_statsbomb_xg)
# Solo para eventos de tipo "Shot"

# 3. Eficiencia de pase (calidad técnica)
pass_completion_rate = (
    count(pass_outcome == "Complete") /
    count(type == "Pass")
)
```

**Transformaciones**:
```python
# Manejo de valores nulos
xg_avg = fillna(0.0)  # Sin disparos → 0 xG
pass_completion_rate = fillna(0.0)  # Sin pases → 0

# Normalización (en inferencia)
from sklearn.preprocessing import StandardScaler
features_scaled = scaler.fit_transform(features)
```

### 5. Random Forest Configuration

```python
RandomForestClassifier(
    n_estimators=10,      # 10 árboles
    max_depth=5,          # Profundidad máxima
    random_state=42,      # Reproducibilidad
    n_jobs=-1,            # Usa todos los cores
    class_weight='balanced'  # Balanceo de clases
)
```

**¿Por qué Random Forest?**
- ✅ Robusto a outliers
- ✅ No requiere feature scaling
- ✅ Maneja interacciones no lineales
- ✅ Proporciona feature importance
- ✅ Rápido en inferencia

**Alternativas consideradas**:
- ❌ Logistic Regression: Asume linealidad
- ❌ XGBoost: Más pesado en inferencia
- ❌ Neural Networks: Overkill para 3 features

### 6. Checkpointing y Fault Tolerance

**¿Qué se guarda en checkpoints?**
```
/app/data/checkpoint/
├── commits/           # Batches procesados exitosamente
├── offsets/           # Última posición en Kafka
├── metadata/          # Info del stream
└── state/             # Estado de agregaciones
```

**Recovery automático**:
1. Si Spark falla, al reiniciar lee checkpoint
2. Encuentra último offset procesado en Kafka
3. Resume desde ahí (exactly-once semantics)

**Limpieza de checkpoints**:
```bash
# Si quieres re-procesar desde el inicio
rm -rf /app/data/checkpoint/*
# Spark arrancará desde "earliest" offsets
```

---

## 📊 Monitoreo y Debugging

### 1. Spark UI (http://localhost:4040)

**Secciones Importantes**:

#### Jobs Tab
```
- Total jobs ejecutados
- Duración de cada job
- Stages involucrados
- Tasks success/failed
```

#### Stages Tab
```
- Detalles de cada stage
- Task distribution
- Input/output size
- Shuffle read/write
- Tiempo por task
```

#### Streaming Tab
```
- Input rate (eventos/segundo)
- Processing time (ms/batch)
- Scheduling delay
- Batch duration
- Queued batches (debe ser 0)
```

**Métricas Clave a Monitorear**:
| Métrica | Valor Saludable | Valor Problemático |
|---------|-----------------|-------------------|
| Processing Time | < Trigger Interval (10s) | > Trigger Interval |
| Scheduling Delay | ~0 ms | > 1000 ms |
| Queued Batches | 0 | > 0 (se acumulan) |
| Task Failures | 0 | > 0 |
| GC Time | < 10% total | > 20% total |

### 2. Logs de Servicios

```bash
# Ver logs de cada servicio
docker logs spark-app
docker logs spark-master
docker logs spark-worker
docker logs kafka
docker logs zookeeper

# Seguir logs en tiempo real
docker logs -f spark-app

# Ver últimas 100 líneas
docker logs --tail 100 spark-app

# Logs con timestamps
docker logs --timestamps spark-app
```

### 3. Kafka Monitoring

```bash
# Entrar al container de Kafka
docker exec -it kafka bash

# Ver topics
kafka-topics --bootstrap-server localhost:9092 --list

# Describir topic
kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic statsbomb-eventos

# Ver consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Ver lag (eventos sin procesar)
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group spark-streaming-consumer

# Consumir mensajes (debugging)
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic statsbomb-eventos --from-beginning --max-messages 10
```

### 4. Inspección de Datos

```bash
# Ver estructura de Parquet files
python src/inspect_data.py

# Explorar datos raw
python explore_data.py

# Verificar respuesta de Statsbomb API
python src/inspect_sb_data.py
```

**Script de Inspección Personalizado**:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Inspector").getOrCreate()

# Leer Parquet
df = spark.read.parquet("/app/data/raw_events/")

# Estadísticas básicas
print(f"Total eventos: {df.count()}")
print(f"Partidos únicos: {df.select('match_id').distinct().count()}")
print(f"Equipos únicos: {df.select('team').distinct().count()}")

# Schema
df.printSchema()

# Muestra
df.show(5, truncate=False)

# Por tipo de evento
df.groupBy("type").count().orderBy("count", ascending=False).show()

# Por equipo
df.groupBy("team", "match_id").count().show()
```

### 5. Troubleshooting Común

#### Problema: "No hay eventos en Kafka"
```bash
# Verificar productor está corriendo
ps aux | grep productor.py

# Verificar Kafka está recibiendo mensajes
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic statsbomb-eventos --from-beginning --max-messages 1

# Si no hay nada, revisar:
docker logs kafka  # Errores de broker?
python src/check_kafka.py  # Conectividad OK?
```

#### Problema: "Spark no procesa eventos"
```bash
# Verificar Spark está conectado a Kafka
# En logs debe aparecer:
"Subscribed to topic: statsbomb-eventos"

# Si no, verificar:
echo $KAFKA_BOOTSTRAP_SERVERS  # Variable correcta?
docker network inspect nocuda_default  # Containers en misma red?

# Reiniciar consumer
pkill -f procesador_streaming.py
./scripts/start_consumer.sh
```

#### Problema: "OutOfMemoryError en Spark"
```bash
# Reducir memoria del executor
# En spark-defaults.conf:
spark.executor.memory=1g  # Era 2g

# O reducir particiones
spark.sql.shuffle.partitions=2  # Era 4

# O procesar menos eventos por batch
spark.streaming.kafka.maxRatePerPartition=50  # Era 100
```

#### Problema: "Modelo no encontrado en inferencia"
```bash
# Verificar ruta
ls -la /app/data/model/
ls -la /app/data/models/

# Re-entrenar
./scripts/train_model.sh

# Si falla entrenamiento, verificar hay datos
spark-shell --master spark://spark-master:7077
>>> spark.read.parquet("/app/data/raw_events/").count()
```

#### Problema: "CheckpointException"
```bash
# Limpiar checkpoints corruptos
rm -rf /app/data/checkpoint/*

# Reiniciar consumer (empezará desde earliest)
```

---

## 🎯 Casos de Uso

### 1. Comparación de Hardware

**Objetivo**: Medir impacto de CPU/RAM en procesamiento de streaming.

**Setup**:
```bash
# Máquina 1: i7, 32GB RAM
docker-compose up -d

# Máquina 2: i3, 8GB RAM
# Ajustar docker-compose.yml:
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=1g  # Era 2g
    - SPARK_WORKER_CORES=1     # Era 2
```

**Métricas a Capturar**:
1. Spark UI → Streaming Tab:
   - Processing Time (ms/batch)
   - Input Rate (records/sec)
   - Total Delay (ms)

2. Bash:
   ```bash
   # CPU usage
   docker stats spark-worker --no-stream

   # Throughput
   grep "events/second" spark-app.log
   ```

3. Parquet Size:
   ```bash
   du -sh /app/data/raw_events/
   ```

**Resultados Esperados**:
| Métrica | i7/32GB | i3/8GB | Diferencia |
|---------|---------|--------|-----------|
| Processing Time | ~2s | ~8s | 4x slower |
| Input Rate | 200/s | 50/s | 4x lower |
| Memory Used | 3GB | 1.5GB | 2x less |

### 2. Análisis de Partidos en Tiempo Real

**Objetivo**: Dashboard live de estadísticas de partido.

**Implementación**:
```python
# En procesador_streaming.py, modificar process_batch():

def process_batch(batch_df, batch_id):
    stats = batch_df.groupBy("team").agg(
        count("*").alias("touches"),
        countDistinct("player").alias("players_active"),
        avg("pass_length").alias("avg_pass_length"),
        sum(when(col("type") == "Shot", 1)).alias("shots"),
        avg("shot_statsbomb_xg").alias("xg_total")
    )

    # Guardar a JSON para dashboard
    stats.coalesce(1).write.mode("overwrite").json("/app/data/live_stats.json")

    # O publicar a otro Kafka topic
    stats.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", "live-stats") \
        .save()
```

**Frontend** (opcional):
```javascript
// Leer JSON cada 10s
setInterval(() => {
  fetch('/data/live_stats.json')
    .then(r => r.json())
    .then(data => updateDashboard(data));
}, 10000);
```

### 3. Entrenamiento Continuo del Modelo

**Objetivo**: Mejorar modelo con datos recientes.

**Cron Job**:
```bash
# Entrenar cada 6 horas con datos nuevos
0 */6 * * * docker exec spark-app bash -c './scripts/train_model.sh'
```

**Script Modificado** (`train_model.sh`):
```bash
# Solo entrenar si hay datos nuevos
MIN_EVENTS=10000
CURRENT_EVENTS=$(spark-sql -e "SELECT COUNT(*) FROM parquet.\`/app/data/raw_events\`")

if [ $CURRENT_EVENTS -gt $MIN_EVENTS ]; then
  echo "Training with $CURRENT_EVENTS events..."
  spark-submit src/entrenamiento_ml.py
else
  echo "Not enough data yet ($CURRENT_EVENTS < $MIN_EVENTS)"
fi
```

### 4. Detección de Anomalías

**Objetivo**: Alertar sobre eventos inusuales en tiempo real.

**Implementación**:
```python
# Añadir a process_batch():

# Detectar tasa de disparos anormal
shots_rate = batch_df.filter(col("type") == "Shot").count() / batch_df.count()

if shots_rate > 0.15:  # >15% son disparos (inusual)
    print(f"⚠️ ALERTA: Tasa de disparos alta: {shots_rate:.2%}")
    # Enviar notificación, email, etc.

# Detectar xG extremo
max_xg = batch_df.agg(max("shot_statsbomb_xg")).collect()[0][0]
if max_xg and max_xg > 0.8:
    player = batch_df.filter(col("shot_statsbomb_xg") == max_xg) \
                     .select("player").first()[0]
    print(f"🎯 Clara ocasión de gol: {player} (xG={max_xg:.2f})")
```

### 5. Exportación para BI Tools

**Objetivo**: Usar datos en Tableau, Power BI, etc.

**Opción 1: CSV**
```python
df = spark.read.parquet("/app/data/raw_events/")
df.coalesce(1).write.mode("overwrite").csv("/app/data/export/events.csv", header=True)
```

**Opción 2: PostgreSQL** (añadir a docker-compose.yml):
```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: football
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
    ports:
      - "5432:5432"
```

```python
# Escribir a Postgres
df.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://postgres:5432/football") \
  .option("dbtable", "events") \
  .option("user", "user") \
  .option("password", "pass") \
  .mode("append") \
  .save()
```

---

## 🚀 Próximos Pasos y Mejoras

### Mejoras de Corto Plazo
1. **Modelo Real**: Usar resultados reales en lugar de labels sintéticas
2. **Más Features**: Añadir distancia recorrida, presión recibida, etc.
3. **Visualización**: Dashboard con Grafana o Streamlit
4. **Testing**: Unit tests para transformaciones críticas

### Mejoras de Largo Plazo
1. **Escalabilidad**: Múltiples workers, particionamiento Kafka
2. **Real-Time ML**: Online learning con Spark Streaming
3. **Multi-Tenancy**: Procesar múltiples ligas simultáneamente
4. **API REST**: Endpoint para consultar predicciones

---

## 📚 Recursos Adicionales

### Documentación Oficial
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Statsbomb Open Data](https://github.com/statsbomb/open-data)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

### Archivos Internos del Proyecto
- [README.md](README.md) - Documentación principal
- [docs/PROJECT_SUMMARY.md](docs/PROJECT_SUMMARY.md) - Resumen técnico
- [docs/QUICK_START.md](docs/QUICK_START.md) - Guía rápida

### Comandos Útiles
```bash
# Resumen del proyecto
cat README.md

# Ver configuración actual
cat .env
cat config/spark/spark-defaults.conf

# Estado de servicios
docker-compose ps
docker stats

# Limpieza completa
docker-compose down -v
rm -rf data/*
```

---

## ✅ Checklist de Dominio del Proyecto

Para considerarte experto en este proyecto, asegúrate de poder:

### Arquitectura
- [ ] Explicar el flujo completo de datos (API → Kafka → Spark → Parquet)
- [ ] Dibujar la arquitectura de Docker Compose con los 5 servicios
- [ ] Describir qué hace cada archivo en `src/`

### Tecnologías
- [ ] Explicar por qué se usa Kafka vs direct streaming
- [ ] Describir cómo Spark Structured Streaming procesa micro-batches
- [ ] Entender el formato Parquet y sus ventajas

### Operación
- [ ] Ejecutar el pipeline completo desde cero
- [ ] Interpretar las métricas del Spark UI
- [ ] Troubleshoot errores comunes (OOM, Kafka down, checkpoint corrupto)

### ML
- [ ] Explicar el feature engineering aplicado
- [ ] Justificar la elección de Random Forest
- [ ] Modificar el modelo para usar otras features

### Código
- [ ] Leer y entender `procesador_streaming.py` completamente
- [ ] Modificar el schema para añadir nuevos campos
- [ ] Implementar una nueva agregación en `process_batch()`

### Performance
- [ ] Recopilar métricas de throughput y latencia
- [ ] Comparar rendimiento entre configuraciones
- [ ] Optimizar configuración de Spark para tu hardware

---

**¡Felicidades!** Ahora tienes una guía completa del proyecto. 🎉

Para dudas o mejoras, consulta:
- Logs: `./scripts/view_logs.sh`
- Spark UI: http://localhost:4040
- Documentación interna: `docs/`

**Happy Streaming!** ⚡🏈
