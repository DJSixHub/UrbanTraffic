# TrafficFlow · Plataforma Distribuida

TrafficFlow levanta una cadena moderna de ingesta: generadores sintéticos por región publican eventos en un clúster Kafka tolerante a fallos, un pipeline redundante escribe los resultados en HDFS (capas *silver* y *gold*) y un dashboard Streamlit ofrece visibilidad en tiempo real. El `docker-compose.yaml` se regenera con `scripts/generate_pipeline.py` para mantener servicios y límites alineados con los perfiles vigentes.

## Requisitos previos
- Docker Desktop (o Engine 24+) con Docker Compose v2
- Python 3.11+ para ejecutar `scripts/generate_pipeline.py`
- (Opcional) `pandas` instalado para recalcular perfiles desde CSV

## Pipeline por secciones
### 1. Preparación de artefactos
- Ejecuta `python scripts/generate_pipeline.py` para calcular perfiles regionales a partir de `data/raw/clean_data.csv` o, si falta, del `fallback_distributions.json`.
- El script refresca `docker-compose.yaml`, crea la estructura de `data/` usada por cada servicio y documenta el origen de los perfiles en `data/generated_profiles/profile_status.json`.

### 2. Infraestructura base
- `docker compose up -d --build` levanta HDFS (namenode y datanodes), ejecuta la preparación inicial mediante `tf-hdfs-bootstrap` y arranca el clúster Kafka en modo Kraft.
- Los volúmenes montados sobre `./data` exponen las salidas de HDFS al host y conservan el estado entre reinicios.

### 3. Generación de eventos
- Cada contenedor `tf-generator-<slug>` lee su perfil, calcula la carga esperada por minuto y publica eventos JSON en `traffic.raw.<región>`.
- Cuando Kafka no responde, los generadores almacenan temporalmente los eventos en `data/producer_spool/<región>` y los reenvían al recuperar la conectividad.

### 4. Ingesta y persistencia
- `tf-pipeline-primary` y `tf-pipeline-backup` comparten el grupo `trafficflow-pipeline`, procesan los tópicos regionales y escriben la capa *silver* particionada por fecha y hora.
- Tras cada lote consolidado, calculan agregados *gold* por región (`role=primary`) y por autoridad local (`role=authority`). Si HDFS no está disponible, el lote se conserva en `data/pipeline_spool` hasta poder persistirlo.

### 5. Observabilidad y análisis
- `tf-dashboard` expone `http://localhost:8501`, consulta WebHDFS con `WEBHDFS_URL` y presenta métricas globales, regionales y de autoridad local cuando los agregados están disponibles.
- El dashboard muestra el origen de los perfiles activos y permite validar visualmente el avance del pipeline sin intervenir contenedores.

### 6. Apagado controlado
- `docker compose down` detiene los servicios conservando volúmenes; añade `-v` para una limpieza completa (útil al regenerar perfiles o reiniciar Kafka desde cero).
- Si necesitas reiniciar solo una parte de la cadena, usa `docker compose stop <servicio>` seguido de `docker compose start <servicio>`.

## Componentes principales
- **HDFS** – `tf-namenode` publica WebHDFS (`9870`) y `tf-datanode*` replica los bloques; el bootstrap inicial crea `/data/silver` y `/data/gold`.
- **Kafka** – `tf-kafka-primary`, `tf-kafka-secondary` y `tf-kafka-tertiary` operan en modo Kraft con réplica interna y exponen `9092` desde el líder.
- **Generadores** – Contenedores `tf-generator-<slug>` leen `data/generated_profiles/distributions.json`, producen eventos y mantienen respaldo en `data/synthetic/<región>`.
- **Pipeline** – `tf-pipeline-primary` y `tf-pipeline-backup` consumen tópicos regionales, controlan `BATCH_SIZE`, `FLUSH_INTERVAL_SECONDS` y persisten metadatos en `data/pipeline_status/`.
- **Dashboard** – Servicio Streamlit (`tf-dashboard`) que transforma datos *gold* en vistas agregadas, gráficos y tablas.

## Tolerancia a fallos
- **Kafka con réplica interna** asegura quórum con tres nodos; la cadena `KAFKA_BOOTSTRAP_SERVERS` de los productores detecta al líder vigente.
- **Spool en productores** evita pérdida de eventos cuando Kafka cae guardándolos en disco y reprocesándolos al reconectar.
- **Pipeline activo-activo** mantiene dos consumidores sincronizados; si uno se detiene, el otro asume todas las particiones sin intervención manual.
- **Dashboard sin estado** puede reiniciarse en cualquier momento y reconstruye su vista leyendo directamente de HDFS.

## Datos y perfiles
- `data/generated_profiles/distributions.json` contiene las distribuciones vigentes para cada región y carretera.
- `data/generated_profiles/profile_status.json` registra si los perfiles provienen de CSV o del conjunto de respaldo.
- `data/synthetic/*.jsonl` almacena copias locales de los eventos emitidos, útiles para reprocesos o auditorías.

## Límites de recursos por servicio
| Servicio | RAM | CPU |
| --- | --- | --- |
| `tf-generator-*` | 96 MiB | 0.15 |
| `tf-pipeline-primary` / `tf-pipeline-backup` | 320 MiB | 0.45 |
| `tf-kafka-primary` / `tf-kafka-secondary` / `tf-kafka-tertiary` | 512 MiB | 0.40 |
| `tf-dashboard` | sin límite | (sin límite) |
| `tf-namenode` / `tf-datanode*` | según imagen base | según imagen base |

## Diagnóstico rápido
- `data/pipeline_status/pipeline-primary.json` y `.../pipeline-backup.json` muestran el último lote procesado por cada consumidor.
- Si un generador reinicia en bucle, revisa permisos de `data/generated_profiles` y el estado del clúster Kafka (`docker compose ps tf-kafka-*`).
- Cuando aparezca `NoBrokersAvailable`, asegúrate de que al menos dos nodos Kafka estén levantados y compartan el mismo `KAFKA_KRAFT_CLUSTER_ID`.
- Lotes en espera se acumulan en `data/pipeline_spool`; se vacían automáticamente al recuperar WebHDFS.
- El dashboard refleja si los perfiles actuales provienen de CSV o del fallback para identificar el origen de la simulación.

## Comandos por etapa
| Etapa | Objetivo | Comando |
| --- | --- | --- |
| 1. Preparación | Regenerar perfiles y `docker-compose.yaml` | `python scripts/generate_pipeline.py` |
| 2. Infraestructura | Arrancar todos los servicios (con build) | `docker compose up -d --build` |
| 3. Generadores | Seguir la producción de una región en vivo | `docker compose logs tf-generator-london -f --tail 80` |
| 4. Pipeline | Revisar ingestión y flushes a HDFS | `docker compose logs tf-pipeline-primary -f --tail 120` |
| 5. Observabilidad | Ver actividad y errores del dashboard | `docker compose logs tf-dashboard -f --tail 80` |
| 6. Persistencia | Validar que existan particiones *silver* | `docker compose exec tf-namenode hdfs dfs -ls /data/silver/regions` |
| 7. Kafka | Listar tópicos y confirmar quórum | `docker compose exec tf-kafka-primary kafka-topics.sh --bootstrap-server tf-kafka-primary:9092 --list` |
| 8. Estado general | Ver contenedores activos y su estado | `docker compose ps` |
| 9. Apagado | Detener servicios y eliminar volúmenes | `docker compose down -v` |
| 10. Spool local | Revisar lotes pendientes en disco | `Get-ChildItem data\pipeline_spool` |

Con estas secciones y comandos puedes desplegar la plataforma completa, supervisar cada tramo del pipeline y realizar tareas de mantenimiento sin depender de artefactos o flujos obsoletos.
