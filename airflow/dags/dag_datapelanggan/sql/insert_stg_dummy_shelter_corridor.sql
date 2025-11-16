-- TRUNCATE TABLE stg.dummy_shelter_corridor;
-- INSERT INTO stg.dummy_shelter_corridor (
--     shelter_name_var,
--     corridor_code,
--     corridor_name,
--     job_id,
--     load_datetime
--     )
CREATE TABLE stg.dummy_shelter_corridor AS
SELECT
    DISTINCT
    shelter_name_var,
    corridor_code,
    corridor_name,
    TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta', 'YYYYMMDDHH24MISS') AS job_id,
    CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS load_datetime
FROM tmp.dummy_shelter_corridor;
