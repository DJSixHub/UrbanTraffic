# TrafficFlow · Plataforma Distribuida

TrafficFlow levanta una cadena moderna de ingesta: generadores sintéticos por región publican eventos en Kafka, un pipeline los consolida en HDFS (*silver/gold*) y un dashboard Streamlit permite inspeccionar la actividad en tiempo real. Todo se orquesta desde un `docker-compose.yaml` autogenerado por `scripts/generate_pipeline.py`, de modo que el entorno siempre refleja el dataset y los límites vigentes.

## Requisitos previos
- Docker Desktop (o Engine 24+) con Docker Compose v2
- Python 3.11+ para correr `scripts/generate_pipeline.py`
- (Opcional) `pandas` instalado para recalcular perfiles desde CSV

## Flujo operativo
1. **Generar artefactos** – `python scripts/generate_pipeline.py` construye perfiles regionales a partir de `data/raw/clean_data.csv` (o cae al `fallback_distributions.json`), prepara directorios de estado y reescribe `docker-compose.yaml` con los servicios y límites actualizados.
2. **Arrancar la plataforma** – `docker compose up -d --build` compila las imágenes locales, inicia Hadoop (namenode/datanode) y Kafka, ejecuta el *bootstrap* de HDFS y luego enciende generadores, pipeline y dashboard en orden.
3. **Producción de eventos** – Cada contenedor `tf-generator-<región>` lee su perfil, genera eventos JSON (1 por vehículo) y:
	- publica individualmente en Kafka (`traffic.raw.<región>`),
	- mantiene archivos rotados en `data/synthetic/<región>` como respaldo.
4. **Procesamiento en Kafka ➜ HDFS** – `tf-pipeline` acumula los eventos por lotes, persiste la capa *silver* particionada (`/data/silver/regions/<slug>/dt=YYYYMMDD/hour=HH/`) y, tras cada descarga, calcula agregados *gold* de dos niveles:
	- `role=primary`: totales por región,
	- `role=authority`: totales por autoridad local dentro de cada región.
	El estado de la corrida queda en `data/pipeline_status/pipeline.json`.
5. **Visualización** – `tf-dashboard` (Streamlit en `http://localhost:8501`) lee la capa *gold* vía WebHDFS, muestra métricas globales o por región y permite bajar al detalle de autoridad local cuando los agregados existen.
6. **Apagado y limpieza** – `docker compose down` detiene los servicios; añade `-v` para descartar volúmenes y reiniciar desde cero (útil para rehacer perfiles o limpiar Kafka).

## Componentes y responsabilidades

**Hadoop + Kafka**
- `tf-namenode` / `tf-datanode`: exponen HDFS (WebHDFS 9870) y montan `./data` para compartir resultados con el host.
- `tf-hdfs-bootstrap`: job efímero que crea `/data/{bronze,silver,gold}` y rutas auxiliares.
- `tf-kafka`: broker Bitnami en modo Kraft con auto creación de tópicos habilitada.

**Generadores**
- 11 containers `tf-generator-<slug>` (uno por región) leen el perfil correspondiente, calculan vehículos por minuto a partir de distribuciones horarias, eligen carretera ponderada y emiten el evento.
- Variables clave: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `ROTATE_RECORDS`, `RATE_PER_MINUTE`.

**Pipeline**
- `tf-pipeline` consume los tópicos definidos en `KAFKA_TOPICS`, controla la ventana de flush con `BATCH_SIZE` y `FLUSH_INTERVAL_SECONDS`, escribe silver/gold en HDFS y actualiza el JSON de estado.
- Las filas *gold* incluyen campos `role`, `region_name`, `local_authority_name` (cuando aplica), totales, promedios y desgloses por categoría de vehículo.

**Dashboard**
- `tf-dashboard` usa WebHDFS (`WEBHDFS_URL`) para leer los archivos *gold* y convierte los registros en vistas agregadas, gráficos de barras/lineas, tortas y tablas. La sección de autoridad local solo aparece si existen registros `role=authority`.

## Datos y perfiles
- `data/generated_profiles/distributions.json` describe velocidades base, desvíos y distribuciones por región/carretera.
- `scripts/generate_pipeline.py` puede reconstruir esos perfiles desde CSV (requiere `pandas`) y guarda la fuente en `data/generated_profiles/profile_status.json` para que el dashboard muestre la procedencia.
- Los archivos `data/synthetic/*.jsonl` almacenan copias locales de los eventos emitidos (útiles para depuración o cargas offline).

## Tasas de generación estimadas
| Región | Vehículos/min (media) | Carreteras |
| --- | ---: | ---: |
| East Midlands | 118 | 247 |
| East of England | 156 | 271 |
| London | 179 | 308 |
| North East | 74 | 135 |
| North West | 215 | 378 |
| Scotland | 74 | 355 |
| South East | 293 | 369 |
| South West | 123 | 309 |
| Wales | 56 | 235 |
| West Midlands | 155 | 249 |
| Yorkshire and The Humber | 138 | 216 |

La generación real oscila alrededor de la media usando la desviación estándar horaria; las cifras anteriores sirven como referencia de carga.

## Límites de recursos por servicio
| Servicio | RAM | CPU |
| --- | --- | --- |
| `tf-generator-*` | 96 MiB | 0.15 |
| `tf-pipeline` | 320 MiB | 0.45 |
| `tf-dashboard` | sin límite | (sin límite) |
| `tf-kafka` | 512 MiB | 0.40 |
| `tf-namenode` / `tf-datanode` | según imagen base | según imagen base |

> **Sugerencia para hardware ajustado (~6 GiB RAM):** apaga temporalmente algunas regiones (`docker compose stop tf-generator-...`), reduce `KAFKA_TOPICS` o elimina volúmenes con `down -v` para evitar acumulación.

## Comandos útiles
- `python scripts/generate_pipeline.py` – Regenera perfiles y compose.
- `docker compose up -d --build` – Compila imágenes locales e inicia todos los servicios.
- `docker compose ps` – Revisa estado general de contenedores.
- `docker compose logs tf-generator-london -f --tail 80` – Sigue un generador (sustituye región según necesidad).
- `docker compose logs tf-pipeline -f --tail 120` – Observa ingestión, flushes y errores de HDFS.
- `docker compose logs tf-dashboard -f --tail 80` – Verifica peticiones WebHDFS y renders del dashboard.
- `docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list` – Lista tópicos disponibles.
- `docker compose exec namenode hdfs dfs -ls /data/silver/regions` – Confirma que los lotes lleguen a HDFS.
- `docker compose exec namenode hdfs dfs -tail /data/gold/management/primary/<archivo>.jsonl` – Inspecciona agregados recientes.
- `docker compose down` / `docker compose down -v` – Apaga la pila (con o sin limpieza de volúmenes).

## Estructura clave
- `scripts/generate_pipeline.py` – Generador de perfiles y compose.
- `producer_service/` – Servicio de generación sintética (Kafka + archivos).
- `pipeline_service/` – Consumidor Kafka ➜ HDFS (*silver/gold*).
- `dashboard/` – Aplicación Streamlit (monitor en vivo).
- `data/` – Perfíl generado, salidas sintéticas, estados y métricas.

## Diagnóstico rápido
- `data/pipeline_status/pipeline.json` muestra el último lote procesado y los tópicos configurados.
- El dashboard indica si el perfil activo proviene de CSV (override) o del fallback.
- Si los generadores reinician, revisa permisos de `data/generated_profiles` (requieren lectura) y la resolución de `tf-kafka`.
- Si el pipeline reporta `NoBrokersAvailable`, verifica que Kafka esté escuchando en `PLAINTEXT://tf-kafka:9092` (regenerar compose incluye el nombre correcto).
- Para limpiar archivos *gold* atascados, elimina `/data/gold/management/primary/*.jsonl` mediante `hdfs dfs -rm` desde el contenedor `tf-namenode`.

Con estos pasos puedes reconstruir el entorno, monitorear la ingestión y diagnosticar problemas sin depender de herramientas externas.
