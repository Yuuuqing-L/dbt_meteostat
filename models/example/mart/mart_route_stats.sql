SELECT
	origin,
	dest,
	count(*) total_flights,
	count (DISTINCT tail_number) unique_airplanes,
	count (DISTINCT airline) unique_airlines,
	round(avg(actual_elapsed_time)) avg_elapsed_time,
	round(avg(arr_delay)) avg_arr_delay,
	max(arr_delay) max_arr_delay,
	min(arr_delay)min_arr_delay,
	sum(
CASE 
	WHEN cancelled = 1 THEN 1 ELSE 0 END ) AS cancelled,
	sum(
CASE 
	WHEN diverted = 1 THEN 1 ELSE 0 END ) AS diverted
FROM {{ ref('prep_flights') }} pf
LEFT JOIN {{ ref('prep_airports') }} pa
ON
	pf.origin = pa.faa
GROUP BY
	origin,
	dest