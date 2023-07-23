{{ config(
    materialized='incremental',
    partition_by={
          "field": "processed_ts",
          "data_type": "timestamp",
          "granularity": "day"
        }
) }}

SELECT
        boca_srgt_key,
        building_id,
        property_name,
        property_ref,
        floor_id,
        floor_name,
        floor_number,
        one_card_or_sensor_peak,
        date_of_card_or_sensor_peak,
        bcd_date,
        design_occ_divers_seat_count,
        design_occ_vent_and_cooling,
        risk_managed_occupancy,
        means_of_escape_capacity,
        toilet_provision,
        ventilation_capacity,
        bcd_updated_by,
        failure_reason,
        processed_file_name,
        processed_ts
  FROM {{ ref('tops_boca_daily_staging_temp') }} temp
where array_length(temp.failure_reason) > 0

{% if is_incremental() %}
    and temp.processed_ts > ( SELECT COALESCE(max(processed_ts), TIMESTAMP('2000-01-01 00:00:00.0000')) from {{ this }} )
{% endif %}