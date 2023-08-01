select
	name,
	branch,
	id,
	year
from {{ ref('testing_raw_temp') }} temp
where temp.failure_reason is null