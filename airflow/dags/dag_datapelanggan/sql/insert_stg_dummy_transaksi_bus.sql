-- TRUNCATE TABLE stg.dummy_transaksi_bus;
-- INSERT INTO stg.dummy_transaksi_bus (
--     uuid,
--     waktu_transaksi,
--     armada_id_var,
--     no_body_var,
--     card_number_var,
--     card_type_var,
--     balance_before_int,
--     fare_int,
--     balance_after_int,
--     transcode_txt,
--     gate_in_boo,
--     p_latitude_flo,
--     p_longitude_flo,
--     status_var,
--     free_service_boo,
--     insert_on_dtm,
--     job_id,
--     load_datetime
-- )
CREATE TABLE stg.dummy_transaksi_bus AS
SELECT
    DISTINCT
    uuid,
    waktu_transaksi,
    armada_id_var,
    left(regexp_replace(upper(no_body_var), '[^A-Z]', '', 'g'), 3) ||
    '-' ||
    lpad(substring(no_body_var from '([0-9]+)'), 3, '0')
    AS no_body_var,
    card_number_var,
    card_type_var,
    balance_before_int,
    fare_int,
    balance_after_int,
    transcode_txt,
    gate_in_boo,
    p_latitude_flo,
    p_longitude_flo,
    status_var,
    free_service_boo,
    insert_on_dtm,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta', 'YYYYMMDDHH24MISS') AS job_id,
    CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS load_datetime
FROM tmp.dummy_transaksi_bus;
