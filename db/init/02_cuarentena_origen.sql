ALTER TABLE cuarentena ADD COLUMN IF NOT EXISTS origen_topic     TEXT;
ALTER TABLE cuarentena ADD COLUMN IF NOT EXISTS origen_partition INT;
ALTER TABLE cuarentena ADD COLUMN IF NOT EXISTS origen_offset    BIGINT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_cuarentena_origen
    ON cuarentena (origen_topic, origen_partition, origen_offset);
