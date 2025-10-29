# UrbanTraffic / TrafficFlow – Streaming Data Lab

Este repositorio contiene un entorno reproducible basado en Docker para generar, almacenar y visualizar datos sintéticos de tráfico utilizando Hadoop (HDFS + YARN), Spark y un dashboard Streamlit. El productor escribe continuamente en HDFS mediante WebHDFS y también deja una copia local para depuración.

## Requisitos

- Docker Desktop (o Docker Engine) 24+
- Docker Compose v2
- Opcional: Python 3.11+ si deseas ejecutar utilidades fuera de los contenedores

## Vista general del stack

Servicios definidos en `docker-compose.yml`:

- `namenode` / `datanode`: HDFS 3.2 con WebHDFS habilitado.
- `resourcemanager` / `nodemanager`: YARN para ejecutar jobs batch (Spark).
- `spark-master` / `spark-worker`: clúster Spark standalone listo para `spark-submit`.
- `hdfs-bootstrap`: job efímero que prepara `/data/gold/synthetic` en HDFS.
- `profile-builder`: job efímero que busca CSV en `data/raw/` y genera un perfil JSON para el productor.
- `producer`: generador sintético de tráfico que rota archivos y los sube a HDFS.
- `dashboard`: Streamlit que consume HDFS en tiempo casi real e informa qué perfil está activo.
## Flujo de trabajo detallado

1. **Inicialización de HDFS**  
   - Contenedor: `hdfs-bootstrap` (basado en la imagen Hadoop NameNode).  
   - Script: `scripts/bootstrap_hdfs.sh` (montado como solo lectura).  
   - Acciones: espera a que el NameNode salga de *safe mode*, crea `/data/gold/synthetic` y ajusta permisos. Vive dentro del clúster HDFS (misma red que `namenode` y `datanode`).

2. **Generación de perfiles desde CSV raw**  
   - Contenedor: `profile-builder` (imagen Spark Master ejecutada como job efímero).  
   - Script principal: `analytics/jobs/build_producer_profiles.py` ejecutado con `spark-submit` en modo local dentro del contenedor.  
   - Origen de datos: bind mount `./data/raw` accesible como `/opt/raw` (host). No se copia a HDFS.  
   - Salida: `./data/generated_profiles/distributions.json` y `profile_status.json`, compartidos mediante bind mount con el productor y el dashboard.  
   - Rol en el clúster: usa el runtime Spark empaquetado en el contenedor; no depende del Spark Master del stack, pero comparte la red Hadoop.

3. **Servicio de producción de eventos**  
   - Contenedor: `producer` (imagen Python construida en `./producer_service`).  
   - Código clave: `producer_service/app/main.py` y `webhdfs_client.py`.  
   - Entrada: perfiles (`/opt/producer/generated/distributions.json` si existe, de lo contrario `/opt/producer/profiles/distributions.json`).  
   - Flujo de datos: genera 1500 registros/minuto; cada 500 registros rota y escribe un archivo local (`./data/synthetic/traffic_stream.jsonl`, bind mount) y a HDFS (`/data/gold/synthetic/dt=YYYYMMDD/...`) mediante WebHDFS.  
   - Entorno: contenedor propio, conectado a la red Hadoop para hablar con NameNode/WebHDFS.

4. **Almacenamiento en Hadoop**  
   - Contenedores: `namenode` y `datanode` (clúster HDFS).  
   - Los archivos JSONL generados viven dentro de HDFS y se persisten en los volúmenes Docker `namenode`/`datanode`.

5. **Dashboard en tiempo real**  
   - Contenedor: `dashboard` (imagen Streamlit, carpeta `./dashboard`).  
   - Código clave: `dashboard/app.py`.  
   - Lectura: usa WebHDFS (`LISTSTATUS` + `OPEN`) para descubrir y leer los últimos archivos bajo `/data/gold/synthetic`.  
   - Indicador de perfil: lee `./data/generated_profiles/profile_status.json` (bind mount) y muestra si el perfil proviene de CSV, de un override previo o de los defaults.  
   - Visualizaciones: Altair y Plotly sobre la ventana móvil de datos.

6. **Jobs batch opcionales**  
   - Scripts en `analytics/jobs/data_cleaning.py` y `exploratory_queries.py`.  
   - Ejecución: `docker compose exec spark-master spark-submit --master yarn ...`. Usan el clúster Spark (`spark-master`/`spark-worker`) apoyado en YARN (`resourcemanager`/`nodemanager`) y leen/escriben datasets en HDFS (`hdfs:///...`).

### Mapa de servicios y roles

- **Clúster HDFS**: `namenode`, `datanode`, `hdfs-bootstrap` (solo inicialización).  
- **YARN**: `resourcemanager`, `nodemanager` (gestión de recursos para Spark en modo YARN).  
- **Spark standalone**: `spark-master`, `spark-worker` (aceptan `spark-submit`; `profile-builder` reutiliza la imagen).  
- **Aplicaciones auxiliares**: `producer` (generación continua), `dashboard` (consumo en vivo).
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
    generated_profiles/       # Perfiles calculados automáticamente al arrancar
BD_Proyecto/
  ...                         # CSV originales si deseas experimentos batch
```

## Puesta en marcha rápida

```powershell
cd TrafficFlow
docker compose up -d --build
```

## Comandos Docker útiles

- Levantar todo con recompilación local: `docker compose up --build`
- Arrancar en segundo plano sin reconstruir: `docker compose up -d`
- Ver logs continuos del productor: `docker compose logs -f producer`
- Listar archivos en HDFS: `docker compose exec namenode hdfs dfs -ls /data/gold/synthetic`
- Mostrar el final de un lote HDFS: `docker compose exec namenode hdfs dfs -tail /data/gold/synthetic/dt=YYYYMMDD/traffic_stream_...jsonl`
- Ejecutar un job Spark batch: `docker compose exec spark-master spark-submit --master yarn --deploy-mode client /opt/spark-apps/data_cleaning.py ...`
- Detener servicios conservando datos: `docker compose down`
- Reinicio limpio (borra volúmenes HDFS): `docker compose down -v`
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
- `PROFILE_OVERRIDE_PATH`: ruta del perfil generado automáticamente (usado por productor y dashboard)
- `PROFILE_STATUS_PATH`: archivo JSON con el estado del perfil activo que el dashboard muestra

Puedes ajustar estos valores en `docker-compose.yml` y volver a levantar con `docker compose up -d --build`.

## Troubleshooting

- **Safe mode**: si ves `Name node is in safe mode`, espera unos segundos o ejecuta `docker compose logs namenode --tail 20` para confirmar que haya salido.
- **Dashboard vacío**: confirma que el productor está escribiendo (`docker compose logs producer --tail 50`) y que existen archivos bajo `/data/gold/synthetic/dt=YYYYMMDD`.
- **Puertos ocupados**: cierra servicios que usen 8501/9870/8088/8080 antes de levantar el stack.

---

Este documento se mantiene alineado con la rama `reset-main`. Si cambias la arquitectura (por ejemplo, añades nuevos consumidores), actualiza este README y `docker-compose.yml` en conjunto.
