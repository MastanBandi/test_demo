{{ config(
    materialized='incremental',
    partition_by={
          "field": "year",
          "data_type": "timestamp",
          "granularity": "day"
        }
) }}
	
select
	name,
	branch,
	id,
	year
from {{ ref('testing_raw_temp') }} temp
where temp.failure_reason is null

{% if is_incremental() %}
    and temp.year > ( select COALESCE(max(year), TIMESTAMP('2000-01-01 00:00:00.0000')) from {{ this }} )
{% endif %}
