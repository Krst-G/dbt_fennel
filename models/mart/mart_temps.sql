select city
		,month_of_year
		,num_month
		,round(avg(max_temp_c),2) as max_temp_c
		,round(avg(min_temp_c),2) as min_temp_c
		,round(avg(avg_temp_c),2) as avg_temp_c 
from mart_forecast_day 
group by city
	,month_of_year
	,num_month 
order by city asc
	,num_month asc;