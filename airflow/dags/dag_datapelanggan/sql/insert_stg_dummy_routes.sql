-- TRUNCATE TABLE stg.dummy_routes;
-- INSERT INTO stg.dummy_routes (
--     route_code,
--     route_name,
--     job_id,
--     load_datetime
-- )
CREATE TABLE stg.dummy_routes AS
SELECT
    DISTINCT
    route_code,
    route_name,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta', 'YYYYMMDDHH24MISS') AS job_id,
    CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS load_datetime
FROM tmp.dummy_routes;
