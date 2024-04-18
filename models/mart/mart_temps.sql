select date
		,year
		,month_of_year
		,month   --- newly added
		,city
		,country
		,round(avg(max_temp_c),2) as max_temp_c
		,round(avg(min_temp_c),2) as min_temp_c
		,round(avg(avg_temp_c),2) as avg_temp_c 
		,lat
        ,lon
        ,timezone_id
from {{ref('mart_forecast_day')}} 
group by date
	,year
	,month_of_year
	,month      --- group on
	,city
	,country	--- group on that too
	,lat
    ,lon
    ,timezone_id
order by city asc    -- optional
	,year asc
	,month asc     --- order by month