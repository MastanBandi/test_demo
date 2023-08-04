{{ config(
    materialized='incremental',
    partition_by={
          "field": "processed_ts",
          "data_type": "timestamp",
          "granularity": "day"
        }
) }}

select
	testing_srgt_key,
	name,
	branch,
	id,
	year,
	processed_file_name,
	processed_ts
from {{ ref('testing_raw_temp') }} temp
where array_length(temp.failure_reason) > 0

{% if is_incremental() %}
    and temp.processed_ts > ( SELECT COALESCE(max(processed_ts), TIMESTAMP('2000-01-01 00:00:00.0000')) from {{ this }} )
{% endif %}
