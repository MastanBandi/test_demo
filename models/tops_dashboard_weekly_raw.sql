{{ config(materialized='table') }}

with source_data as (
	SELECT
		*
		{{ dbt_utils.generate_surrogate_key(['name','id']) }} as testing_srgt_key
		from {{ source('dmn01_finsoi_bqd_bandi','testing_table') }} raw
),

validated_data as (
	SELECT
		*,
        [
            IF( SAFE_CAST(coalesce(name,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("name" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",name,"]") as error)),
            IF( SAFE_CAST(coalesce(branch,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("branch" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",branch,"]") as error)),
            IF( SAFE_CAST(coalesce(id,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("id" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",id,"]") as error)),
            IF( SAFE_CAST(coalesce(year,'01/01/1001') AS DATE FORMAT 'DD/MM/YYYY') is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("year" as col,  CONCAT("dtypeCheckFailed [invalid date] [value: ",year,"]") as error)),
	] as validation_reason
	from source_data
),

error_data as (
	SELECT testing_srgt_key,
		ARRAY_AGG(error) as failure_reason,
       from validated_data, unnest(validation_reason) as error
             where error.col != 'NA'
       group by testing_srgt_key
)

select
	name,
	branch,
	id,
	year
from source_data sd left join error_data ed
on sd.testing_srgt_key= ed.testing_srgt_key
