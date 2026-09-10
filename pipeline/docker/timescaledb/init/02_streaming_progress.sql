-- Progreso de cada micro-lote de Spark Structured Streaming.

CREATE TABLE IF NOT EXISTS streaming_progress (
    -- Instante en que Spark emitio el informe de progreso.
    trigger_ts    TIMESTAMPTZ NOT NULL,

    run_id        TEXT        NOT NULL,
    query_name    TEXT        NOT NULL,
    batch_id      BIGINT      NOT NULL,

    num_input_rows            BIGINT,
    input_rows_per_second     DOUBLE PRECISION,
    processed_rows_per_second DOUBLE PRECISION,

    duration_ms               BIGINT,
    add_batch_ms              BIGINT,
    query_planning_ms         BIGINT,

    -- rows_dropped_by_watermark > 0 significa que Spark esta desechando eventos por
    -- llegar tarde
    -- offsets_behind es lo que Kafka tiene y spark aun no ha leido
    rows_dropped_by_watermark  BIGINT,
    offsets_behind             BIGINT,

    event_time_max            TIMESTAMPTZ,
    watermark                 TIMESTAMPTZ,

    PRIMARY KEY (trigger_ts, run_id, query_name, batch_id)
);

SELECT create_hypertable(
    'streaming_progress',
    'trigger_ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- Patron de consulta del informe de KPIs: "todos los lotes de la ultima
-- ejecucion, por consulta".
CREATE INDEX IF NOT EXISTS idx_progress_run
    ON streaming_progress (run_id, query_name, trigger_ts DESC);

COMMENT ON TABLE streaming_progress IS
    'Progreso por micro-lote de Spark';
