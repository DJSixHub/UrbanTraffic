# UrbanTraffic / TrafficFlow – Streaming Data Lab

Este repositorio contiene un entorno reproducible basado en Docker para generar, almacenar y visualizar datos sintéticos de tráfico utilizando Hadoop (HDFS + YARN), Spark y un dashboard Streamlit. El productor escribe continuamente en HDFS mediante WebHDFS y también deja una copia local para depuración.

## Requisitos

- Docker Desktop (o Docker Engine) 24+
- Docker Compose v2
- Opcional: Python 3.11+ si deseas ejecutar utilidades fuera de los contenedores

## Vista general del stack

Servicios definidos en `docker-compose.yml`:

- `namenode` / `datanode`: HDFS 3.2 con WebHDFS habilitado
- `resourcemanager` / `nodemanager`: YARN para ejecutar jobs batch (Spark)
- `spark-master` / `spark-worker`: Spark 3.2 listo para `spark-submit`
- `hdfs-bootstrap`: job efímero que prepara `/data/gold/synthetic` en HDFS
- `producer`: generador sintético de tráfico que rota archivos y los sube a HDFS
- `dashboard`: aplicación Streamlit que lee HDFS en tiempo casi real y refresca cada 2 s

## Estructura del repositorio

```
TrafficFlow/
  docker-compose.yml          # Orquestación de todo el stack
  analytics/
    jobs/
      data_cleaning.py        # Ejemplo de job Spark batch
      exploratory_queries.py  # Consultas exploratorias sobre los datos limpios
  producer_service/
    app/main.py               # Lógica del productor y writers (file/HDFS)
  dashboard/
    app.py                    # Dashboard Streamlit apuntando a WebHDFS
    requirements.txt          # Dependencias del contenedor de dashboard
  data/
    synthetic/                # Salida local del productor (bind mount)
BD_Proyecto/
  ...                         # CSV originales si deseas experimentos batch
```

## Puesta en marcha rápida

```powershell
cd TrafficFlow
docker compose up -d --build
```

La primera vez el dashboard instalará dependencias (incluye matplotlib) y los contenedores de Hadoop tardan unos segundos en salir de *safe mode*. Mientras tanto verás mensajes `Name node is in safe mode`. Vuelve a intentarlo tras ~30 s; el productor reintentará automáticamente.

Un contenedor auxiliar `hdfs-bootstrap` espera a que HDFS salga de *safe mode* y crea la jerarquía `/data/gold/synthetic`, ajustando su propiedad a `hdfs:hdfs`. Así evitamos pasos manuales después de un reinicio limpio (`docker compose down -v`).

Servicios expuestos:

- Dashboard → http://localhost:8501
- NameNode UI → http://localhost:9870
- ResourceManager UI → http://localhost:8088
- Spark Master UI → http://localhost:8080

## Verificando que todo corre

```powershell
# Logs del productor (confirmar cargas a HDFS)
docker compose logs producer --tail 50

# Logs del dashboard
docker compose logs dashboard --tail 20

# Archivos generados en HDFS (WebHDFS path)
docker compose exec namenode hdfs dfs -ls /data/gold/synthetic

# Contenido local (útil para inspección rápida)
Get-Content data/synthetic/traffic_stream.jsonl -Tail 5
```

El dashboard muestra:

- Conteos del número de registros y vehículos en la ventana móvil (15 min)
- Serie temporal de vehículos por minuto (resample de los eventos)
- Barras y gráficos de pastel por región y tipo de vehículo
- Selector de región para ver el desglose de vehículos pesados/ligeros

## Ejecutar jobs Spark batch

Los ejemplos de `analytics/jobs` siguen disponibles. Primero asegúrate de tener datos en HDFS (puedes utilizar los CSV de `BD_Proyecto` o reutilizar los archivos *gold* generados por el productor).

```powershell
# Limpieza a silver (ejemplo)
docker compose exec spark-master \
  spark-submit \
    --master yarn \
    --deploy-mode client \
    /opt/spark-apps/data_cleaning.py \
    --input-path hdfs:///data/gold/synthetic \
    --output-path hdfs:///data/silver/traffic_clean

# Consultas exploratorias
docker compose exec spark-master \
  spark-submit \
    --master yarn \
    --deploy-mode client \
    /opt/spark-apps/exploratory_queries.py \
    --input-path hdfs:///data/silver/traffic_clean
```

Ajusta `--input-path` según el origen deseado (raw, silver o gold).

## Detener y limpiar

```powershell
# Detener servicios preservando datos de HDFS y el archivo local
docker compose down

# Detener y eliminar volúmenes HDFS (reinicio limpio)
docker compose down -v
```

Los datos locales en `data/synthetic/traffic_stream.jsonl` se mantienen porque es un bind mount. Para limpiar ese archivo manualmente:

```powershell
Clear-Content data/synthetic/traffic_stream.jsonl
```

## Variables relevantes

- `RATE_PER_MINUTE`: ritmo de generación del productor (registros/minuto)
- `ROTATE_RECORDS`: cuántos registros antes de subir un nuevo archivo a HDFS
- `HDFS_BASE_PATH`: ruta base donde aterrizan los archivos en HDFS (`/data/gold/synthetic` por defecto)
- `STREAM_WINDOW_MINUTES`: ventana mostrada en el dashboard (15 min)
- `STREAM_REFRESH_SECONDS`: intervalo fijo de refresco del dashboard (2 s)

Puedes ajustar estos valores en `docker-compose.yml` y volver a levantar con `docker compose up -d --build`.

## Troubleshooting

- **Safe mode**: si ves `Name node is in safe mode`, espera unos segundos o ejecuta `docker compose logs namenode --tail 20` para confirmar que haya salido.
- **Dashboard vacío**: confirma que el productor está escribiendo (`docker compose logs producer --tail 50`) y que existen archivos bajo `/data/gold/synthetic/dt=YYYYMMDD`.
- **Puertos ocupados**: cierra servicios que usen 8501/9870/8088/8080 antes de levantar el stack.

---

Este documento se mantiene alineado con la rama `reset-main`. Si cambias la arquitectura (por ejemplo, añades Kafka u otros consumidores), actualiza este README y `docker-compose.yml` en conjunto.
