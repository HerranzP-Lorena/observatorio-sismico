-- Catalogo de paises (Natural Earth). Clave: ADM0_A3
CREATE TABLE pais (
    adm0_a3     TEXT PRIMARY KEY,
    nombre_es   TEXT NOT NULL,
    admin       TEXT,
    continente  TEXT,
    subregion   TEXT
);

-- Estado actual: una fila por terremoto (ultima version)
CREATE TABLE evento_actual (
    event_id     TEXT PRIMARY KEY,
    time_utc     TIMESTAMPTZ NOT NULL,
    updated_utc  TIMESTAMPTZ NOT NULL,
    mag          NUMERIC(4,2),
    mag_type     TEXT,
    depth_km     NUMERIC(8,3),
    lon          DOUBLE PRECISION NOT NULL,
    lat          DOUBLE PRECISION NOT NULL,
    place        TEXT,
    pais_adm0_a3 TEXT REFERENCES pais(adm0_a3),
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (lon BETWEEN -180 AND 180 AND lat BETWEEN -90 AND 90)
);

-- Historial: una fila por version (event_id, updated_utc)
CREATE TABLE evento_version (
    event_id     TEXT NOT NULL,
    updated_utc  TIMESTAMPTZ NOT NULL,
    time_utc     TIMESTAMPTZ NOT NULL,
    mag          NUMERIC(4,2),
    mag_type     TEXT,
    depth_km     NUMERIC(8,3),
    lon          DOUBLE PRECISION,
    lat          DOUBLE PRECISION,
    place        TEXT,
    pais_adm0_a3 TEXT,
    original     JSONB NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, updated_utc)
);

-- Registros excluidos o invalidos, con motivo y original
CREATE TABLE cuarentena (
    id           BIGSERIAL PRIMARY KEY,
    event_id     TEXT,
    categoria    TEXT NOT NULL,
    motivo       TEXT NOT NULL,
    original     JSONB NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Registro de consultas a las fuentes
CREATE TABLE ingesta_consulta (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL,
    url           TEXT NOT NULL,
    requested_at  TIMESTAMPTZ NOT NULL,
    http_status   INT,
    n_records     INT,
    error         TEXT
);

-- Estado del productor: ultima version publicada de cada evento
CREATE TABLE evento_publicado (
    event_id               TEXT PRIMARY KEY,
    last_updated_published TIMESTAMPTZ NOT NULL
);

-- Indices para los filtros del panel (fecha, pais, magnitud)
CREATE INDEX idx_evento_actual_time ON evento_actual (time_utc);
CREATE INDEX idx_evento_actual_pais ON evento_actual (pais_adm0_a3);
CREATE INDEX idx_evento_actual_mag  ON evento_actual (mag);
