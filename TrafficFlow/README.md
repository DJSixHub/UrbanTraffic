# TrafficFlow – Infraestructura Batch sobre Hadoop/Spark

Este proyecto provee un entorno reproducible para trabajar con el dataset de tráfico del Department for Transport (DfT) del Reino Unido utilizando HDFS, YARN y Spark (batch). Los objetivos cubiertos en esta fase son:

1. **Infraestructura funcional**: cluster Hadoop + Spark listo para ejecutar jobs de MapReduce/Spark.
2. **Carga inicial**: ingestión de un subconjunto del dataset en HDFS.
3. **Limpieza y transformación básica**: job batch que normaliza los datos crudos hacia una zona *silver*.
4. **Consultas exploratorias**: job batch con agregaciones iniciales por región, autoridad local y tipo de carretera.

> El análisis en tiempo real y los componentes de streaming se abordarán en fases posteriores.

## Requisitos

- Docker 24+
- Docker Compose v2
- Python 3.12+ (solo para utilidades locales)
- Dataset CSV ubicado en `BD_Proyecto/` (carpeta existente en el repositorio)

## Estructura

```
TrafficFlow/
  docker-compose.yml        # Define HDFS, YARN y Spark (master + worker)
  analytics/
    jobs/
      data_cleaning.py      # Limpieza y enriquecimiento del dataset crudo -> silver (Parquet)
      exploratory_queries.py # Consultas iniciales sobre el dataset silver
  data/
    raw/                    # Carpeta local donde colocar el CSV crudo (se monta en los contenedores)
    silver/                 # Carpeta local usada para exportar resultados (opcional)
BD_Proyecto/
  dft_traffic_counts_raw_counts.csv
  local_authority_traffic.csv
  region_traffic.csv
```

## 1. Levantar la infraestructura

Desde la raíz del repositorio:

```powershell
cd TrafficFlow
docker compose up -d
```

Servicios disponibles:

- NameNode UI → http://localhost:9870
- ResourceManager UI → http://localhost:8088
- Spark Master UI → http://localhost:8080

## 2. Copiar un subset del dataset a HDFS

1. Copia el CSV crudo al volumen local (solo la primera vez):

   ```powershell
   Copy-Item ..\BD_Proyecto\dft_traffic_counts_raw_counts.csv .\data\raw\
   ```

2. Crea la carpeta en HDFS y sube el archivo (puedes tomar un subset con `Select-Object` si deseas reducir tamaño):

   ```powershell
   # Crear carpeta destino en HDFS
   docker compose exec namenode hdfs dfs -mkdir -p /data/raw

   # Subir el CSV crudo (editar la ruta si generaste un subset)
   docker compose exec namenode hdfs dfs -put -f /data/raw/dft_traffic_counts_raw_counts.csv /data/raw/
   ```

## 3. Ejecutar la limpieza básica (Spark batch)

El job lee el CSV crudo, castea columnas, calcula métricas derivadas y guarda el resultado en formato Parquet particionado por año y región.

```powershell
docker compose exec spark-master \
  spark-submit \
    --master yarn \
    --deploy-mode client \
    /opt/spark-apps/data_cleaning.py \
    --input-path hdfs:///data/raw/dft_traffic_counts_raw_counts.csv \
    --output-path hdfs:///data/silver/dft_traffic_clean \
    --repartition 24
```

Los datos limpios quedarán en `hdfs:///data/silver/dft_traffic_clean/`.

## 4. Consultas exploratorias iniciales

El siguiente job calcula KPIs básicos (top regiones, densidad promedio, ranking de autoridades locales y estadísticas por tipo de carretera).

```powershell
docker compose exec spark-master \
  spark-submit \
    --master yarn \
    --deploy-mode client \
    /opt/spark-apps/exploratory_queries.py \
    --input-path hdfs:///data/silver/dft_traffic_clean \
    --top-n 10
```

Las salidas se muestran en consola de Spark. Opcionalmente, puedes persistir los resultados en HDFS añadiendo `--output-path hdfs:///data/silver/analytics`.

## 5. Apagar la infraestructura

```powershell
docker compose down -v
```

Esto eliminará contenedores y volúmenes HDFS. Si deseas conservar los datos, omite `-v`.

## 6. Dashboard en tiempo real (opcional)

1. Instala las dependencias locales (solo una vez):

  ```powershell
  cd TrafficFlow
  python -m venv .venv
  .venv\Scripts\Activate.ps1
  pip install -r dashboard/requirements.txt
  ```

2. Asegúrate de que el productor esté generando datos (`docker compose up -d` → el archivo `data/synthetic/traffic_stream.jsonl`).

3. Ejecuta el dashboard Streamlit:

  ```powershell
  streamlit run dashboard/app.py
  ```

  La página se refresca automáticamente y muestra totales por minuto, acumulados por región y la tabla más reciente en una ventana deslizante (configurable desde la barra lateral).

---

Con esta base, la siguiente fase consistirá en añadir el productor distribuido, la cola de mensajería y el dashboard unificado que combine métricas batch y streaming.
