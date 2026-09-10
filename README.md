# Sistema escalable de microservicios para el procesamiento y análisis de datos IoT

Pipeline de telemetría IoT containerizado e implementado con herramientas de código abierto, que sigue una **arquitectura Kappa**. El caso de uso es la supervisión de consumo energético de edificios: telemetría de medidores de electricidad, agua fría y agua caliente.

## Visión general de la arquitectura

El sistema implementa una arquitectura Kappa de flujo único con una bifurcación final hacia dos sumideros. Los componentes son servicios independientes, desacoplados mediante contenedores Docker, y la comunicación entre productores y consumidores de datos se ejecuta usando Kafka como log reproducible, mientras que la gobernanza de esquemas se resuelve a través del registro Apicurio.

El simulador (`mqtt_simulator.py`) publica telemetría hacia Eclipse Mosquitto, que actúa únicamente como broker de ingesta MQTT. El bridge, un microservicio de diseño propio, se suscribe a los mensajes desde Mosquitto, los serializa contra Apicurio Registry conforme al esquema Avro vigente y los publica en Kafka.

Kafka es un log persistente de bytes crudos, ajeno a la estructura interna de los mensajes almacenados y distribuidos. Es por ello que el bridge, en la escritura, y Spark Structured Streaming, en la lectura, son quienes dialogan directamente con Apicurio para serializar y deserializar las cadenas de bytes almacenadas y distribuidas por Kafka.

Spark Structured Streaming consume cada mensaje mediante **dos consultas independientes** sobre el mismo flujo de Kafka: en una de ellas enriquece los mensajes y agrega métricas por ventana temporal hacia TimescaleDB para el consumo operacional en Grafana, y en la otra consulta guarda los mensajes hacia PostgreSQL para el consumo analítico con Power BI. Al tratarse de consultas independientes, cada una mantiene su propio *checkpoint* y su propia tolerancia a fallos, de modo que la caída de una consulta no interrumpe a la otra.

```
Simulador → Mosquitto → Bridge → Kafka → Spark Structured Streaming ─┬→ TimescaleDB → Grafana
                                    ↑                                 └→ PostgreSQL → Power BI
                                 Apicurio
                                (esquemas)
```

## Fuente de datos

Los datos provienen de la competición Kaggle ASHRAE -- Great Energy Predictor III, un subconjunto del proyecto Building Data Genome 2 que registra el consumo energético por hora de 1.449 edificios reales en 16 emplazamientos durante 2016, con errores de medición (no hay datos sintéticos).

El script de preparación (`prepare_ashrae.py`) reduce el dataset original a un subconjunto de tres emplazamientos (2, 3 y 5; 652 medidores y 5.682.185 lecturas de consumo), y produce tres tablas Parquet:

- Una **tabla de hechos** con la lectura de cada medidor (`building_id`, `meter_type`, `timestamp`, `meter_reading`).
- Una **tabla de dimensión** con los atributos de cada edificio (`building_id`, `site_id`, `primary_use`, `square_feet`, `year_built`, `floor_count`).
- Una **tabla de línea base** con los cuartiles históricos de cada medidor (`baseline_p75`, `baseline_iqr`), usada para la detección de picos atípicos.

## Componentes del sistema

**Simulador de telemetría** (`mqtt_simulator.py`) — ocupa el lugar de los 652 medidores reales del subconjunto. Cada medidor publica una vez por hora, lo que supone 0,18 eventos por segundo considerando el total de medidores; el argumento `--acelerar` incrementa el ritmo de publicación durante las pruebas de carga. Cada evento se publica en formato JSON en texto plano, con los campos `building_id`, `meter_type`, `timestamp`, `meter_reading` y `sim_publish_ts` (instante de publicación, usado para medir la latencia de persistencia).

**Broker MQTT (Eclipse Mosquitto)** — actúa únicamente como broker de ingesta MQTT: recibe la telemetría publicada por el simulador y la deja disponible para que el bridge la consuma, sin ejecutar ninguna lógica o transformación.

**Bridge MQTT → Kafka** (`mqtt_kafka_bridge.py`) — se suscribe a los mensajes `iot/#` en Mosquitto, valida cada mensaje contra el esquema Avro vigente en Apicurio Registry, lo serializa a una cadena de bytes y lo publica en Kafka. Los mensajes que no superan la validación no se descartan: se desvían al tópico de mensajes inválidos (`iot.telemetry.dlq`, una *Dead Letter Queue*) junto con el motivo del rechazo, el tópico MQTT de origen (por ejemplo `iot/346/electricity/telemetry`) y un timestamp de cuando fue procesado. Un mensaje llega a la DLQ por dos vías: si no encaja en el esquema Avro, o si no supera una validación adicional de dominio implementada en el propio bridge (una fecha futura que dañaría el *watermark* de Spark, o una lectura negativa o no finita). Del lado de Kafka, el bridge publica con `acks='all'`, el nivel de confirmación más estricto. La clave con la que publica cada mensaje es el propio sensor (`building_id:meter_type`), lo que mantiene en la misma partición y en orden las lecturas de cada medidor. El bridge escala horizontalmente: el argumento `--shared-group` le pide al broker Mosquitto que reparta los mensajes entre varias instancias del bridge en vez de duplicarlos, de forma que `docker compose up -d --scale bridge=N` levanta N réplicas que se reparten el flujo.

**Registro de esquemas (Apicurio Registry)** — gobierna el contrato de datos del proyecto. El esquema Avro inicial (`telemetry_event_v1.avsc`) se registra como paso manual e independiente, antes del arranque del pipeline, siguiendo la práctica recomendada de no dejar que cada productor de mensajes se autoregistre. Apicurio comprueba que cada esquema nuevo sea sintácticamente válido y compatible con todas las versiones anteriores; si no pasa esas comprobaciones, rechaza el registro. El bridge y Spark obtienen su copia del esquema (o de todos los esquemas registrados, en el caso de Spark) una sola vez al arrancar, y no vuelven a llamar a Apicurio por cada mensaje.

**Apache Kafka** — el log persistente de bytes crudos sobre el que se apoya la arquitectura Kappa. Se despliega en modo KRaft como un solo nodo, combinando los roles de *broker* y *controller*. Cada mensaje se almacena en su partición según la clave `building_id:meter_type`, garantizando que las lecturas de un mismo medidor se procesen en el orden en que se publicaron. Se usan dos tópicos: `iot.telemetry.raw` (mensajes válidos) e `iot.telemetry.dlq` (mensajes rechazados), ambos con retención de siete días.

**Apache Spark Structured Streaming** (`stream_processing.py`) — el único consumidor de Kafka. Sus dos consultas de flujo independientes deserializan cada mensaje Avro con el esquema correspondiente a su `schema_id`, los enriquecen mediante un *broadcast join* con las tablas de dimensión y línea base, y los guardan en sus respectivos sumideros. La consulta hacia TimescaleDB agrega por ventanas temporales de una hora sobre el tiempo de evento, con *watermarking* para el tratamiento de datos tardíos; la consulta hacia PostgreSQL almacena cada evento enriquecido de forma directa. La escritura en ambos sumideros (`database_writers.py`) usa un `UPSERT` (`INSERT ... ON CONFLICT DO UPDATE`) sobre la clave natural de cada tabla, haciendo idempotente el reprocesamiento del log de Kafka.

**TimescaleDB** — sumidero operacional. Contiene `telemetry_metrics` (una fila por ventana de una hora, emplazamiento, uso de edificio y tipo de medidor) y `streaming_progress` (progreso de cada micro-lote de las dos consultas de Spark).

**PostgreSQL** — sumidero analítico. Contiene `buildings` y `sensor_baseline` (las tablas de referencia) y `telemetry_events` (los eventos individuales enriquecidos, sin ningún campo calculado).

**Grafana** — se conecta a TimescaleDB para tres *dashboards* (Estado del pipeline, Consumo energético, Calidad y anomalías) con acceso anónimo de solo lectura habilitado.

**Power BI** — informe en formato PBIP conectado en modo *DirectQuery* a PostgreSQL, con tres páginas: Consumo de Energía, Eficiencia Eléctrica y Detección de Anomalías.

## Decisiones de arquitectura

- **Mosquitto con bridge propio, no un broker con puente nativo a Kafka**: ningún broker MQTT con licencia de código abierto ofrece un puente nativo hacia Kafka (esa función solo existe en las ediciones comerciales de EMQX/HiveMQ). Se optó por un microservicio de *bridging* propio en vez de un conector genérico porque permite asignarle responsabilidades adicionales (validación de esquema, DLQ) sin depender de las limitaciones de configuración de un conector.
- **TimescaleDB, no InfluxDB**: al ser una extensión de PostgreSQL, comparte cliente, dialecto SQL y patrón de escritura *upsert* con el sumidero analítico, evitando incorporar un lenguaje de consulta adicional.
- **Spark Structured Streaming, no PyFlink**: el parque de medidores emite una lectura por hora y sensor, muy por debajo del umbral en el que las ventajas de latencia sub-segundo de un motor de flujo nativo como Flink resultan necesarias. A esto se suma la mayor madurez de la API de PySpark en el ecosistema Python y la unificación del procesamiento por lotes y en flujo bajo el mismo modelo de *DataFrame*.

## Estructura del repositorio

```
tfm_boris_paternina/
|-- setup.sh                    # Crea el entorno virtual, descarga el dataset Kaggle
|                               #   y crea el subconjunto usando prepare_ashrae.py
|-- requirements.txt            # Dependencias Python del pipeline
|-- .sdkmanrc                   # Versión de Java fijada para PySpark
|-- .gitignore
|-- pipeline/
|   |-- data/
|   |   |-- raw/                    # Carpeta de descarga de ASHRAE Kaggle competition
|   |   |-- prepare_ashrae.py       # Genera los archivos .parquet del pipeline
|   |-- docker/
|   |   |-- mosquitto/              # Configuración del broker MQTT
|   |   |-- grafana/                # Datasources y dashboards provisionados
|   |   |-- timescaledb/            # Esquema SQL de métricas y progreso de streaming
|   |   |-- postgres/               # Esquema SQL de eventos enriquecidos
|   |-- simulator/
|   |   |-- mqtt_simulator.py       # Simulador del parque de medidores
|   |   |-- simulator_helper.py     # Carga y reparto del dataset para el simulador
|   |-- bridge/
|   |   |-- mqtt_kafka_bridge.py    # Microservicio bridge MQTT -> Kafka
|   |   |-- Dockerfile              # Imagen del microservicio bridge
|   |-- schemas/
|   |   |-- telemetry_event_v1.avsc # Contrato de datos en Apicurio
|   |   |-- register_schema.py      # Registro y gobernanza de esquemas en Apicurio
|   |-- spark/
|   |   |-- checkpoints/            # Estado de ejecución de las consultas a Kafka
|   |   |-- stream_processing.py    # Script de procesamiento PySpark
|   |   |-- database_writers.py     # Escritura idempotente hacia los sumideros
|   |   |-- monitoring.py           # Supervisión del proceso Spark
|   |-- common/                     # Utilidades compartidas: logging, conexiones, Apicurio
|   |-- logs/                       # Logs de ejecución
|   |-- tools/                      # Scripts de medición (KPIs), prueba de fallos, demo.py
|   |-- .env.example                # Plantilla con credenciales de ejemplo para bases
|   |                               #   de datos y configuración de tópicos Kafka
|   |-- docker-compose.yml          # Orquesta Mosquitto, Bridge, Apicurio, Kafka,
|                                   #   TimescaleDB, PostgreSQL y Grafana
|-- powerbi/
    |-- powerbi_dashboard_analitico.pbip            # Proyecto Power BI (formato PBIP)
    |-- powerbi_dashboard_analitico.Report/         # Definición de páginas y visuales
    |-- powerbi_dashboard_analitico.SemanticModel/  # Modelo semántico: tablas y relaciones
```

## Ejecución

### Requisitos previos

El sistema se ejecuta sobre Linux, WSL2 o macOS y las dependencias en detalle están en `requirements.txt` en la raíz del repositorio:

- Docker y Docker Compose para el stack de servicios.
- Java 17 o superior (21 LTS recomendado).
- Python 3.11.

Con ejecutar `bash setup.sh` en la raíz del repositorio, se comprueba la presencia de Docker y Java (guía la instalación si no los detecta), se crea el entorno virtual `.venv`, se instalan las dependencias de `requirements.txt` y se prepara la data en Parquet si se encuentran las credenciales de Kaggle para descargar el dataset; caso contrario se presenta una guía para obtener y guardar dichas credenciales.

El procedimiento completo desde cero:

1. **Clonar el repositorio.**
   ```bash
   git clone https://github.com/b0risR/tfm-boris-paternina.git
   cd tfm-boris-paternina
   ```

2. **Crear el fichero de configuración.** Copia la plantilla `pipeline/.env.example` a `pipeline/.env`. Es la fuente única de puertos, nombres de base de datos y nombres de tópico que leen tanto `docker-compose.yml` como los procesos del *host* (Spark y las herramientas de `tools/`). Los valores por defecto sirven tal cual; se edita solo para cambiar algún puerto.
   ```bash
   cp pipeline/.env.example pipeline/.env
   ```

3. **Preparar el entorno.**
   ```bash
   bash setup.sh
   ```

4. **Activar el entorno virtual.**
   ```bash
   source .venv/bin/activate
   ```

A partir de aquí la ejecución sigue el procedimiento manual, o puede ejecutarse el modo automático con `python pipeline/tools/demo.py` para una demostración.

Se acceden a los dashboards de Grafana en http://localhost:3000. Para abrir las páginas de Power BI, usar la siguiente ruta desde Archivo →Abrir informe →Examinar informes:
```
\\wsl.localhost\Ubuntu\home\<usuario>\<ruta-al-repo>\powerbi\powerbi_dashboard_analitico.pbip
```

La primera vez que se abre `powerbi_dashboard_analitico.pbip`, Power BI solicita las credenciales de PostgreSQL (no se almacenan en el fichero `.pbip`). Hay que indicar, en la pestaña `Base de datos`, el usuario `tfm` y la contraseña `developer` (los valores por defecto de `pipeline/.env.example`). Si aparece un aviso de compatibilidad de cifrado, se acepta la conexión sin cifrar. El stack debe estar en ejecución para que las páginas de Power BI hagan DirectQuery.

### Modo automático de ejecución

El script `pipeline/tools/demo.py` orquesta el arranque del sistema: levanta el stack Docker, limpia las bases de datos, inicia el proceso Spark, espera a que las consultas hacia Kafka estén activas, y lanza el simulador reproduciendo los eventos más recientes de la data en los archivos Parquet.

```bash
python pipeline/tools/demo.py                 # publica 6 semanas de eventos
python pipeline/tools/demo.py --semanas 10    # publica 10 semanas de eventos
python pipeline/tools/demo.py --stop          # cierra los procesos y baja el stack
```

La opción `--semanas N` reproduce las últimas `N` semanas del subconjunto (valores disponibles entre 1 y 52; 6 por defecto), contadas desde la fecha más reciente hacia atrás, por lo que deben ajustarse los rangos de fechas en Grafana para cubrir las últimas `N` semanas.

Este script sirve para **demostrar** el sistema en funcionamiento, no para medir: las cifras de los KPIs se toman con el procedimiento manual de la siguiente sección.

### Arranque manual (emisión de KPIs)

El arranque manual sigue cinco pasos, en este orden:

1. **Levantar el stack.**
   ```bash
   docker compose -f pipeline/docker-compose.yml up -d
   ```

2. **Estado limpio.** Con el stack en marcha, borra los *checkpoints* de Spark, recrea los tópicos de Kafka y vacía las tablas de los sumideros.
   ```bash
   python pipeline/tools/reset_state.py --yes
   ```

3. **Arrancar Spark.** El primer arranque descarga los conectores de Maven y tarda más de lo habitual; hay que esperar a que Spark informe de que las consultas hacia Kafka están en marcha antes de generar carga.
   ```bash
   python pipeline/spark/stream_processing.py --trigger "1 second"
   ```

4. **Generar la carga.** El simulador reproduce la telemetría de los 652 sensores. El factor `--acelerar` aumenta la velocidad de publicación por segundo calculado como 652 x (N / 3600), y `--limite` acota cuántos eventos se publicarán (sin `--limite` se reproducirán 5.682.185 eventos).
   ```bash
   python pipeline/simulator/mqtt_simulator.py --acelerar 2000 --limite 50000
   ```

5. **Emitir el cuadro de KPIs.** `kpi_report.py` consulta los informes de actividad del pipeline y escribe el reporte en `pipeline/logs/informe_kpi.md`.
   ```bash
   python pipeline/tools/kpi_report.py
   ```

El job de Spark debe estar procesando **antes** de que el simulador publique. Si se altera el orden, los tópicos en Kafka esperan a que aparezca quien los consuma y esa espera incrementa la latencia de persistencia, alterando la medición.

### Escalado horizontal del bridge

La siguiente variación del paso 1 del arranque manual permite la replicación del bridge en `N` instancias:

```bash
docker compose -f pipeline/docker-compose.yml up -d --scale bridge=N
```

Mosquitto se encarga de repartir los mensajes entre las réplicas del bridge. El comando `failover_test.py --target bridge --fallo oom` provoca la caída de una sola, haciendo que las demás asuman la carga de la réplica caída; `restart: unless-stopped` es la política en `docker-compose.yml` que repone esa única réplica automáticamente durante la prueba.

### Herramientas de validación

Las validaciones del pipeline se obtienen mediante cinco scripts que residen en `pipeline/tools/`:

| Herramienta | Función | Salida |
|---|---|---|
| `reset_state.py` | Deja el sistema en estado limpio: borra los *checkpoints* de Spark, recrea los tópicos de Kafka y vacía las tablas de los sumideros. | --- |
| `kpi_report.py` | Interroga PostgreSQL, TimescaleDB, Apicurio y Grafana y emite el cuadro de los KPIs en Markdown. | `pipeline/logs/informe_kpi.md` |
| `failover_test.py` | Lanza el simulador, provoca el fallo de un servicio, mide el tiempo de recuperación y la tasa de pérdida. | `pipeline/logs/ultimo_failover.json` |
| `faulty_simulator.py` | Publica una mezcla de eventos reales e inválidos para ejercitar la validación del esquema Avro. | `pipeline/data/faulty_events.json` |
| `dlq_inspect.py` | Lee el tópico `iot.telemetry.dlq` y muestra el motivo, el tópico de origen MQTT y el payload de cada evento rechazado. | --- |

`kpi_report.py` mide el estado en que encuentra el pipeline en el instante en que es ejecutado. Para que sus cifras sean fiables, debe seguirse estrictamente el orden del arranque manual (stack, estado limpio, Spark, simulador y reporte) antes de ser utilizado.

`failover_test.py` necesita que se haya ejecutado `reset_state.py` antes de iniciar el pipeline. El script verifica que el bridge y Spark estén en marcha antes de interrumpir algún servicio:

```bash
python pipeline/tools/failover_test.py --target mosquitto --fallo kill
python pipeline/tools/failover_test.py --target kafka --fallo kill
python pipeline/tools/failover_test.py --target timescaledb --fallo oom
python pipeline/tools/failover_test.py --target postgres --fallo oom
python pipeline/tools/failover_test.py --target bridge --fallo oom
```

Los servicios `mosquitto` y `kafka` no admiten `--fallo oom`: Mosquitto usa unos 3 MB, insuficiente para un fallo por memoria, y en Kafka un fallo de este tipo deja la JVM en bucle de reinicios.

### Inyección de errores durante la publicación de eventos MQTT

El script `pipeline/tools/faulty_simulator.py` publica una mezcla de eventos reales y eventos inválidos para ejercitar la ruta de validación del bridge y dejar constancia de los rechazos en `iot.telemetry.dlq`.

```bash
python pipeline/tools/faulty_simulator.py                              # 10.000 eventos, 6 inválidos
python pipeline/tools/faulty_simulator.py --limite 20000 --fallas 300  # 20.000 eventos, 300 inválidos
```

`--limite` fija cuántos eventos reales se toman del dataset original (10.000 por defecto), y `--fallas` cuántos eventos inválidos se reparten a partes iguales entre seis motivos de rechazo por el bridge, en posiciones equiespaciadas dentro del lote.

Los seis motivos de rechazo introducidos son:

- `meter_reading` con valor infinito.
- `meter_reading` con valor `NaN`.
- `meter_reading` con valor no numérico.
- `building_id` ausente del evento.
- `building_id` como número en vez de cadena de texto.
- `timestamp` con valor nulo.

De una forma similar a `mqtt_simulator.py` se publican 370 eventos por segundo en su opción por defecto.

### Visualización de los eventos rechazados por el bridge

El script `pipeline/tools/dlq_inspect.py` lee el tópico de Kafka `iot.telemetry.dlq` e imprime en pantalla, por cada evento rechazado, el motivo, el tópico MQTT de origen y el payload original.

```bash
python pipeline/tools/dlq_inspect.py                     # toda la DLQ
python pipeline/tools/dlq_inspect.py --desde AAAA-MM-DD  # solo lo rechazado desde esa fecha
```

`--desde` filtra desde el instante en que el bridge procesó el rechazo, no por el *timestamp* original del evento.

Cada entrada muestra:

- El *offset* del mensaje dentro de su partición en `iot.telemetry.dlq`.
- La fecha y hora del rechazo, en UTC.
- El tópico MQTT de origen, por ejemplo `iot/346/electricity/telemetry`.
- El motivo del rechazo: la excepción de Python y su mensaje.
- El payload original sin alterar, disponible para su reprocesamiento.

Un ejemplo de cómo se vería en pantalla un evento rechazado por el bridge:

```
[0] 2026-09-05T13:17:22Z | topico=iot/222/chilledwater/telemetry
    motivo=ValueError: meter_reading no finito (inf)
    payload: {"building_id": "222", "meter_type": "chilledwater",
              "timestamp": "2026-09-03T00:00:00",
              "meter_reading": Infinity, "sim_publish_ts": 1788614242721}
```

### Arranque con Spark contenedorizado

Las cifras de KPI se toman con Spark en el host (`local[*]` sobre los núcleos del procesador); contenedorizarlo en un solo equipo empeora el rendimiento por contención de recursos. Por eso el `docker compose up -d` por defecto **no** incluye Spark.

Aun así, el pipeline está preparado para contenedorizar Spark sin alterar código. El archivo `pipeline/docker-compose.spark.yml` es un *overlay* que añade el servicio `spark` (imagen en `pipeline/spark/Dockerfile`) apuntando a la red interna de contenedores, más un init `create-topics` que crea los tópicos de Kafka antes de que Spark arranque.

```bash
# Levanta el stack completo con Spark como contenedor (sustituye a los pasos 1 y 3 del arranque manual):
docker compose -f pipeline/docker-compose.yml -f pipeline/docker-compose.spark.yml up -d

# Generar carga:
python pipeline/simulator/mqtt_simulator.py --acelerar 2000 --limite 50000

# Parar y limpiar (limpia el volumen del checkpoint de Spark):
docker compose -f pipeline/docker-compose.yml -f pipeline/docker-compose.spark.yml down -v
```

El escalado horizontal del bridge es independiente de este overlay y puede combinarse con él:

```bash
docker compose -f pipeline/docker-compose.yml -f pipeline/docker-compose.spark.yml up -d --scale bridge=3
```

## Licencia: Código bajo licencia MIT (ver LICENSE).