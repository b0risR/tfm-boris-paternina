-- Eventos individuales enriquecidos (sumidero analitico).
--
-- ---------------------------------------------------------------------------
-- Dimension de edificios
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS buildings (
    building_id   TEXT     PRIMARY KEY,
    site_id       INTEGER  NOT NULL,
    primary_use   TEXT     NOT NULL,
    square_feet   INTEGER,
    year_built    INTEGER,   -- ausente en 184 de los 498 edificios
    floor_count   INTEGER    -- ausente en 409 de los 498
);

-- ---------------------------------------------------------------------------
-- Linea base por sensor
-- ---------------------------------------------------------------------------
-- Se cargan aqui para que Power BI pueda marcar por si mismo
-- las lecturas atipicas con un join
CREATE TABLE IF NOT EXISTS sensor_baseline (
    building_id    TEXT             NOT NULL,
    meter_type     TEXT             NOT NULL,
    baseline_p25   DOUBLE PRECISION,
    baseline_p50   DOUBLE PRECISION,
    baseline_p75   DOUBLE PRECISION,
    baseline_iqr   DOUBLE PRECISION,
    PRIMARY KEY (building_id, meter_type)
);

-- ---------------------------------------------------------------------------
-- Hechos: lecturas de contador
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telemetry_events (
    building_id          TEXT             NOT NULL,
    meter_type           TEXT             NOT NULL,
    event_time           TIMESTAMPTZ      NOT NULL,
    meter_reading        DOUBLE PRECISION,

    -- Instrumentacion del KPI de latencia (Objetivo 1)
    sim_publish_ts       TIMESTAMPTZ,
    persisted_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),

    PRIMARY KEY (building_id, meter_type, event_time)
);

CREATE INDEX IF NOT EXISTS idx_events_time   ON telemetry_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_meter  ON telemetry_events (meter_type, event_time DESC);

COMMENT ON TABLE telemetry_events IS
    'Lecturas de eventos. Escribe Spark, consume Power BI.';
COMMENT ON TABLE sensor_baseline IS
    'Cuartiles historicos por contador. Se carga desde ashrae_sensor_baseline.parquet.';
COMMENT ON TABLE buildings IS
    'Dimension de edificios. Se carga una vez desde ashrae_buildings.parquet.';
