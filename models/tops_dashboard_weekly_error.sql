select
	name,
	branch,
	id,
	year
fron {{ ref('testing_raw_temp') }} temp
where array_length(temp.failure_reason) > 0