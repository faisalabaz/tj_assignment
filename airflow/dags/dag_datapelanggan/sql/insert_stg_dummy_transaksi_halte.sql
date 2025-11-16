-- TRUNCATE TABLE stg.dummy_transaksi_halte;
-- INSERT INTO stg.dummy_transaksi_halte (
--     uuid,
--     waktu_transaksi,
--     shelter_name_var,
--     terminal_name_var,
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
CREATE TABLE stg.dummy_transaksi_halte AS
SELECT
    DISTINCT
    uuid,
    waktu_transaksi,
    shelter_name_var,
    terminal_name_var,
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
FROM tmp.dummy_transaksi_halte;
