# Quick Start Guide

## TL;DR - Inicio Rápido en 4 Pasos

### 1. Levantar el entorno
```bash
chmod +x scripts/*.sh
./scripts/setup.sh
```

### 2. Entrenar el modelo ML
```bash
./scripts/train_model.sh
```

### 3. Iniciar el consumidor Spark (Terminal 1)
```bash
./scripts/start_consumer.sh
```

### 4. Iniciar el productor Kafka (Terminal 2)
```bash
./scripts/start_producer.sh
```

## Ver la Spark UI
Abre en tu navegador: **http://localhost:4040**

---

## Comandos Útiles

### Ver logs de servicios
```bash
# Todos los servicios
./scripts/view_logs.sh

# Un servicio específico
./scripts/view_logs.sh kafka
./scripts/view_logs.sh spark-master
```

### Acceder al contenedor
```bash
docker exec -it pyspark-app bash
```

### Detener todo
```bash
./scripts/stop_services.sh
```

### Limpiar todo (incluyendo datos)
```bash
docker-compose down -v
```

---

## Estructura de Ejecución Recomendada

1. **Terminal 1**: Consumer (Spark Streaming)
2. **Terminal 2**: Producer (Kafka)
3. **Terminal 3**: Logs o comandos adicionales
4. **Navegador**: Spark UI (http://localhost:4040)

---

## Troubleshooting Rápido

### "Connection refused" al iniciar
Espera 1-2 minutos para que Kafka y Spark inicien completamente.

### "No module named 'statsbombpy'"
```bash
docker-compose build --no-cache
docker-compose up -d
```

### Reiniciar desde cero
```bash
docker-compose down -v
rm -rf data/processed/* data/checkpoints/*
./scripts/setup.sh
```

---

## Captura de Métricas para Comparación

### Desde Spark UI (http://localhost:4040)

1. **Jobs Tab** → Captura screenshot
2. **Stages Tab** → Click en cada stage → Captura métricas
3. **Executors Tab** → Captura uso de memoria

### Métricas Clave a Anotar

- ✅ Tiempo total de ejecución
- ✅ Scheduler Delay
- ✅ Executor Run Time
- ✅ GC Time
- ✅ Shuffle Read/Write
- ✅ Spill (Memory/Disk)

---

## Arquitecturas de Comparación

| Máquina | CPU | RAM | Propietario |
|---------|-----|-----|-------------|
| **Arch 1** | Intel i7 (11va) | 32 GB | [Tu nombre] |
| **Arch 2** | Intel i3 | 8 GB | [Compañero] |

**IMPORTANTE**: Ambos deben usar la misma configuración de Spark Worker (2G memory, 2 cores).

---

Para más detalles, consulta el [README.md](README.md) completo.
