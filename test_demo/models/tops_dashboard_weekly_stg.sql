{{ config(
    materialized='incremental',
    partition_by={
          "field": "processed_ts",
          "data_type": "timestamp",
          "granularity": "day"
        }
) }}


select
        boca_srgt_key,
        cast(building_id as int64) as building_id,
        property_name,
        property_ref,
        cast(floor_id as int64) as floor_id,
        floor_name,
        floor_number,
        cast(one_card_or_sensor_peak as int64) as one_card_or_sensor_peak,
        cast(date_of_card_or_sensor_peak as date FORMAT 'DD/MM/YYYY') as date_of_card_or_sensor_peak,
        cast(bcd_date as date FORMAT 'DD/MM/YYYY') as bcd_date,
        cast(design_occ_divers_seat_count as int64) as design_occ_divers_seat_count,
        cast(design_occ_vent_and_cooling as int64) as design_occ_vent_and_cooling,
        cast(risk_managed_occupancy as int64) as risk_managed_occupancy,
        cast(means_of_escape_capacity as int64) as means_of_escape_capacity,
        cast(toilet_provision as int64) as toilet_provision,
        cast(ventilation_capacity as int64) as ventilation_capacity,
        bcd_updated_by,
        processed_file_name,
        processed_ts
from {{ ref('tops_boca_daily_staging_temp') }} temp
where temp.failure_reason is null

{% if is_incremental() %}
    and temp.processed_ts > ( select COALESCE(max(processed_ts), TIMESTAMP('2000-01-01 00:00:00.0000')) from {{ this }} )
{% endif %}