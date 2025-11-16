-- TRUNCATE TABLE stg.dummy_realisasi_bus;
-- INSERT INTO stg.dummy_realisasi_bus (
--     tanggal_realisasi,
--     bus_body_no,
--     rute_realisasi,
--     job_id,
--     load_datetime
-- )
CREATE TABLE stg.dummy_realisasi_bus AS
SELECT
    DISTINCT
    tanggal_realisasi,
    left(regexp_replace(upper(bus_body_no), '[^A-Z]', '', 'g'), 3) ||
    '-' ||
    lpad(substring(bus_body_no from '([0-9]+)'), 3, '0')
    AS bus_body_no,
    rute_realisasi,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta', 'YYYYMMDDHH24MISS') AS job_id,
    CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS load_datetime
FROM tmp.dummy_realisasi_bus;