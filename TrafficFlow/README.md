# TrafficFlow · Plataforma Distribuida

TrafficFlow levanta una cadena moderna de ingesta: generadores sintéticos por región publican eventos en un clúster Kafka tolerante a fallos, un pipeline redundante escribe los resultados en HDFS (*silver/gold*) y un dashboard Streamlit permite inspeccionar la actividad en tiempo real. Todo se orquesta desde un `docker-compose.yaml` autogenerado por `scripts/generate_pipeline.py`, de modo que el entorno siempre refleja el dataset y los límites vigentes.

## Requisitos previos
- Docker Desktop (o Engine 24+) con Docker Compose v2
- Python 3.11+ para correr `scripts/generate_pipeline.py`
- (Opcional) `pandas` instalado para recalcular perfiles desde CSV

## Flujo operativo
1. **Generar artefactos** – `python scripts/generate_pipeline.py` construye perfiles regionales a partir de `data/raw/clean_data.csv` (o cae al `fallback_distributions.json`), prepara directorios de estado y reescribe `docker-compose.yaml` con los servicios y límites actualizados.
2. **Arrancar la plataforma** – `docker compose up -d --build` compila las imágenes locales, inicia Hadoop (namenode/datanode) y Kafka, ejecuta el *bootstrap* de HDFS y luego enciende generadores, pipeline y dashboard en orden.
3. **Producción de eventos** – Cada contenedor `tf-generator-<región>` lee su perfil, genera eventos JSON (1 por vehículo) y:
	- publica individualmente en Kafka (`traffic.raw.<región>`),
	- mantiene archivos rotados en `data/synthetic/<región>` como respaldo,
	- almacena temporalmente los eventos en `data/producer_spool/<región>` si el clúster Kafka está inalcanzable y los reenvía automáticamente al reconectarse.
4. **Procesamiento en Kafka ➜ HDFS** – Los servicios `tf-pipeline-primary` y `tf-pipeline-backup` comparten el mismo *consumer group*; mientras ambos estén activos se reparten las particiones y, si uno cae, el otro asume la totalidad. Cada lote persiste la capa *silver* particionada (`/data/silver/regions/<slug>/dt=YYYYMMDD/hour=HH/`) y, tras cada descarga, calcula agregados *gold* de dos niveles:
	- `role=primary`: totales por región,
	- `role=authority`: totales por autoridad local dentro de cada región.
	El estado de la corrida queda en `data/pipeline_status/pipeline-primary.json` y `pipeline-backup.json`. Si HDFS está inalcanzable, los lotes se estacionan en `data/pipeline_spool` y se vuelven a escribir cuando el almacenamiento responde.
5. **Visualización** – `tf-dashboard` (Streamlit en `http://localhost:8501`) lee la capa *gold* vía WebHDFS, muestra métricas globales o por región y permite bajar al detalle de autoridad local cuando los agregados existen.
6. **Apagado y limpieza** – `docker compose down` detiene los servicios; añade `-v` para descartar volúmenes y reiniciar desde cero (útil para rehacer perfiles o limpiar Kafka).

## Componentes y responsabilidades

**Hadoop + Kafka**
- `tf-namenode` / `tf-datanode`: exponen HDFS (WebHDFS 9870) y montan `./data` para compartir resultados con el host.
- `tf-hdfs-bootstrap`: job efímero que crea `/data/{bronze,silver,gold}` y rutas auxiliares.
- `tf-kafka-primary`, `tf-kafka-secondary` y `tf-kafka-tertiary`: forman un clúster Kafka en modo Kraft con réplica interna; el nodo *primary* expone `9092` al host y todos escuchan internamente en `PLAINTEXT_INTERNAL://tf-kafka-*:9092`.

**Generadores**
- 11 containers `tf-generator-<slug>` (uno por región) leen el perfil correspondiente, calculan vehículos por minuto a partir de distribuciones horarias, eligen carretera ponderada y emiten el evento.
- Variables clave: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `ROTATE_RECORDS`, `RATE_PER_MINUTE` y `PRODUCER_SPOOL_PATH` (cola local de contingencia).

**Pipeline**
- `tf-pipeline-primary` y `tf-pipeline-backup` comparten `KAFKA_GROUP_ID` (`trafficflow-pipeline`), controlan la ventana de flush con `BATCH_SIZE` y `FLUSH_INTERVAL_SECONDS`, y escriben simultáneamente en las capas *silver* y *gold*.
- Cada instancia mantiene su propio archivo de estado y una cola en disco (`data/pipeline_spool`) para retener lotes cuando HDFS no responde.

**Dashboard**
- `tf-dashboard` usa WebHDFS (`WEBHDFS_URL`) para leer los archivos *gold* y convierte los registros en vistas agregadas, gráficos de barras/lineas, tortas y tablas. La sección de autoridad local solo aparece si existen registros `role=authority`.

## Tolerancia a fallos
- **Kafka replica**: el clúster de tres nodos mantiene quórum; si uno cae, los otros dos conservan el servicio y los productores detectan al nuevo líder mediante la cadena configurada en `KAFKA_BOOTSTRAP_SERVERS`.
- **Productores con spool**: cuando Kafka no está disponible, los eventos se guardan en `data/producer_spool/<región>` y se reprocesan automáticamente en cuanto la conexión se restablece.
- **Pipelines activos-activos**: dos consumidores comparten el grupo; si uno se detiene, el otro asume inmediatamente las particiones. Además, cualquier lote que no llegue a HDFS queda almacenado en `data/pipeline_spool` hasta poder escribirlo.
- **Dashboard sin estado**: puede apagarse sin afectar la ingesta; al volver a levantarlo, lee los lotes históricos directamente desde HDFS y reconstruye las vistas.

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
| `tf-pipeline-primary` / `tf-pipeline-backup` | 320 MiB | 0.45 |
| `tf-kafka-primary` / `tf-kafka-secondary` / `tf-kafka-tertiary` | 512 MiB | 0.40 |
| `tf-dashboard` | sin límite | (sin límite) |
| `tf-namenode` / `tf-datanode` | según imagen base | según imagen base |

> **Sugerencia para hardware ajustado (~6 GiB RAM):** apaga temporalmente algunas regiones (`docker compose stop tf-generator-...`), reduce `KAFKA_TOPICS` o elimina volúmenes con `down -v` para evitar acumulación.

## Comandos útiles
- `python scripts/generate_pipeline.py` – Regenera perfiles y compose.
- `docker compose up -d --build` – Compila imágenes locales e inicia todos los servicios.
- `docker compose ps` – Revisa estado general de contenedores.
- `docker compose logs tf-generator-london -f --tail 80` – Sigue un generador (sustituye región según necesidad).
- `docker compose logs tf-pipeline-primary -f --tail 120` – Observa ingestión, flushes y errores de HDFS desde la instancia activa.
- `docker compose logs tf-pipeline-backup -f --tail 40` – Verifica que el respaldo siga en espera y sin errores.
- `docker compose logs tf-dashboard -f --tail 80` – Verifica peticiones WebHDFS y renders del dashboard.
- `docker compose logs tf-kafka-primary -f --tail 80` – Revisa el estado del líder del clúster Kafka.
- `docker compose exec tf-kafka-primary kafka-topics.sh --bootstrap-server tf-kafka-primary:9092 --list` – Lista tópicos desde el clúster.
- `docker compose exec namenode hdfs dfs -ls /data/silver/regions` – Confirma que los lotes lleguen a HDFS.
- `docker compose exec namenode hdfs dfs -tail /data/gold/management/primary/<archivo>.jsonl` – Inspecciona agregados recientes.
- `docker compose down` / `docker compose down -v` – Apaga la pila (con o sin limpieza de volúmenes).
- `ls data/producer_spool/<región>` / `ls data/pipeline_spool` – Comprueba si existen eventos pendientes de reenvío.

## Estructura clave
- `scripts/generate_pipeline.py` – Generador de perfiles y compose.
- `producer_service/` – Servicio de generación sintética (Kafka + archivos).
- `pipeline_service/` – Consumidores Kafka ➜ HDFS (*silver/gold*) con cola persistente.
- `dashboard/` – Aplicación Streamlit (monitor en vivo).
- `data/` – Perfíl generado, salidas sintéticas, estados y métricas.

## Diagnóstico rápido
- `data/pipeline_status/pipeline-primary.json` y `pipeline-backup.json` muestran el último lote procesado por cada instancia.
- El dashboard indica si el perfil activo proviene de CSV (override) o del fallback.
- Si los generadores reinician, revisa permisos de `data/generated_profiles` (requieren lectura) y que el clúster Kafka esté en quórum (`docker compose ps tf-kafka-*`).
- Si un pipeline queda fuera, confirma que el otro siga asignado al grupo (`docker compose logs tf-pipeline-backup`); los lotes pendientes aparecerán en `data/pipeline_spool` hasta completarse.
- Si ves `NoBrokersAvailable`, asegúrate de que al menos dos nodos Kafka estén levantados y que advirtieron la misma configuración; regenerar el compose restablece la lista de votantes.
- Para limpiar archivos *gold* atascados, elimina `/data/gold/management/primary/*.jsonl` mediante `hdfs dfs -rm` desde el contenedor `tf-namenode`.

Con estos pasos puedes reconstruir el entorno, monitorear la ingestión y diagnosticar problemas sin depender de herramientas externas.
