{{ config(materialized='ephemeral') }}

with source_data as (
    SELECT
        *,
    --- generating unique key that can identify each record & used for joins
    {{ dbt_utils.generate_surrogate_key(['building_id','floor_id']) }} as boca_srgt_key
    from {{ source('tops_raw', 'tops_boca_daily_raw') }} raw

  ),

validated_data as (
    select
        *,
        [
            IF( SAFE_CAST(coalesce(building_id,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("building_id" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",building_id,"]") as error)),
            IF( SAFE_CAST(coalesce(floor_id,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("floor_id" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",floor_id,"]") as error)),
            IF( SAFE_CAST(coalesce(one_card_or_sensor_peak,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("one_card_or_sensor_peak" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",one_card_or_sensor_peak,"]") as error)),
            IF( SAFE_CAST(coalesce(date_of_card_or_sensor_peak,'01/01/1001') AS DATE FORMAT 'DD/MM/YYYY') is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("date_of_card_or_sensor_peak" as col,  CONCAT("dtypeCheckFailed [invalid date] [value: ",date_of_card_or_sensor_peak,"]") as error)),
            IF( SAFE_CAST(coalesce(bcd_date,'01/01/1001') AS DATE FORMAT 'DD/MM/YYYY') is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("bcd_date" as col,  CONCAT("dtypeCheckFailed [invalid date] [value: ",bcd_date,"]") as error)),
            IF( SAFE_CAST(coalesce(design_occ_divers_seat_count,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("design_occ_divers_seat_count" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",design_occ_divers_seat_count,"]") as error)),
            IF( SAFE_CAST(coalesce(design_occ_vent_and_cooling,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("design_occ_vent_and_cooling" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",design_occ_vent_and_cooling,"]") as error)),
            IF( SAFE_CAST(coalesce(risk_managed_occupancy,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("risk_managed_occupancy" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",risk_managed_occupancy,"]") as error)),
            IF( SAFE_CAST(coalesce(means_of_escape_capacity,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("means_of_escape_capacity" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",means_of_escape_capacity,"]") as error)),
            IF( SAFE_CAST(coalesce(toilet_provision,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("toilet_provision" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",toilet_provision,"]") as error)),
            IF( SAFE_CAST(coalesce(ventilation_capacity,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("ventilation_capacity" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",ventilation_capacity,"]") as error))
            ] as validation_reason
    from source_data
),

error_data as (
select boca_srgt_key,
       ARRAY_AGG(error) as failure_reason,
       from validated_data, unnest(validation_reason) as error
             where error.col != 'NA'
       group by boca_srgt_key
)

select
        sd.boca_srgt_key,
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
        sd.processed_file_name,
        sd.processed_ts
from source_data sd left join error_data ed
on sd.boca_srgt_key = ed.boca_srgt_key
