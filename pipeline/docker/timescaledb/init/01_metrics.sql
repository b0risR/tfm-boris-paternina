-- Metricas agregadas por ventana temporal

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry_metrics (
    -- Ventana de agregacion (tumbling de 1 hora sobre event time)
    window_start          TIMESTAMPTZ      NOT NULL,
    window_end            TIMESTAMPTZ      NOT NULL,

    site_id               INTEGER          NOT NULL,
    primary_use           TEXT             NOT NULL,
    meter_type            TEXT             NOT NULL,
    event_count           BIGINT           NOT NULL,
    distinct_buildings    BIGINT           NOT NULL,
    avg_reading           DOUBLE PRECISION,
    max_reading           DOUBLE PRECISION,
    sum_reading           DOUBLE PRECISION,
    sum_square_feet       DOUBLE PRECISION,
    avg_energy_intensity  DOUBLE PRECISION,
    zero_count            BIGINT           NOT NULL,
    anomaly_count         BIGINT           NOT NULL,

    -- Instante en que la fila se escribio o actualizo, asignado por la base.
    persisted_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),

    -- Una ventana solo puede tener una fila por combinacion de claves. Es lo que
    -- hace idempotente un reproceso desde Kafka en lugar de duplicar metricas.
    PRIMARY KEY (window_start, site_id, primary_use, meter_type)
);

SELECT create_hypertable(
    'telemetry_metrics',
    'window_start',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE
);

-- Patron de consulta dominante de los dashboards: filtrar por emplazamiento,
-- tipo de contador y rango temporal.
CREATE INDEX IF NOT EXISTS idx_metrics_site_meter_time
    ON telemetry_metrics (site_id, meter_type, window_start DESC);

COMMENT ON TABLE telemetry_metrics IS
    'Agregados por ventana de 1h sobre event time. Escribe Spark, consume Grafana.';
