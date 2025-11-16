import pendulum
from datetime import datetime

local_tz = pendulum.timezone("Asia/Jakarta")

# Define custom macro to get job_id_bq
def job_id_bq(execution_date: pendulum.DateTime) -> str:
    dt_object = datetime.fromtimestamp(execution_date.timestamp(), tz=local_tz)
    job_id = dt_object.strftime("%Y%m%d%H%M%S")
    return job_id