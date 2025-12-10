# Explicación Técnica: Solución de Múltiples Workers en Spark

## El Problema Original
Inicialmente, el sistema estaba configurado con un solo contenedor `spark-worker`. Aunque Spark está diseñado para computación distribuida, al intentar escalar a más workers o asignar más recursos, el sistema fallaba o no se comportaba como se esperaba.

La razón principal (y la que causaba los "crashes" al intentar paralelizar) era la **falta de visibilidad de los datos por parte de los workers**.

### Análisis de la Causa Raíz
En una arquitectura distribuida de Spark, tenemos:
1. **Driver**: Orquesta la tarea (nuestro `src/entrenamiento_ml.py`).
2. **Master**: Asigna recursos.
3. **Workers/Executors**: Ejecutan el código y procesan los datos.

Cuando tu código hace `spark.read.parquet("/app/data/...")`, el Driver le dice a los Executors: *"Vayan y lean este archivo en esta ruta"*.

Si los contenedores de los Workers NO tienen montado el volumen `./data:/app/data`, cuando intentan buscar el archivo `/app/data/...` para procesarlo, fallan con `FileNotFoundException` o errores similares, haciendo que la tarea falle y el Driver termine abortando.

Al tener un solo worker (y si casualmente ese tenía acceso, o si el código se ejecutaba en modo `local` dentro del contenedor `spark-app` sin enviar trabajo real al worker), funcionaba "de milagro" o simplemente no estaba distribuyendo nada.

## La Solución Implementada

Hemos realizado tres cambios clave para permitir una verdadera paralelización estable:

### 1. Definición Explícita de Múltiples Workers en Docker
En lugar de un solo servicio, hemos definido `spark-worker-1` y `spark-worker-2` en el `docker-compose.yml`.
**Lo más importante:** A ambos se les han montado los volúmenes de datos explícitamente:
```yaml
    volumes:
      - ./data:/app/data
      - ./src:/app/src
```
Esto garantiza que cualquiera de los dos workers puede acceder a los archivos `.parquet` que necesita procesar.

### 2. Uso de `spark-submit`
Antes, es probable que ejecutáramos el script simplemente con `python script.py`. Si la sesión de Spark no se configuraba explícitamente para apuntar al Master, a menudo Spark cae en modo `local[*]`, ignorando el cluster por completo.
Hemos actualizado `scripts/train_model.sh` para usar `spark-submit --master spark://spark-master:7077`. Esto **fuerza** al trabajo a enviarse al Master y distribuirse entre los workers disponibles.

### 3. Configuración de Recursos
Hemos limitado cada worker a 1 Core y 1GB de RAM para asegurar que caben cómodamente en tu máquina local sin saturarla (evitando crasheos por falta de memoria en tu PC), pero permitiendo demostrar que hay dos procesos independientes trabajando.

## Conclusión para el Profesor
"Profesor, el problema de los crasheos anteriores era que, al paralelizar, los nuevos workers no tenían acceso físico a los archivos de datos (no compartían el volumen). Hemos corregido la arquitectura contenerizada para que todos los nodos trabajadores tengan acceso compartido al sistema de archivos (`/app/data`), y hemos configurado el envío de tareas explícito al Cluster Manager. Ahora el sistema escala horizontalmente sin errores."
