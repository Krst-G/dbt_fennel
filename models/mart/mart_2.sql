WITH joining_day_location AS (
        SELECT * FROM {{ref('prep_forecast_day')}}
        LEFT JOIN {{ref('staging_location')}}
        USING(city,region,country)
),
filtering_conditions as (
select date
		,year
		,month_of_year
		,month   --- newly added
		,city
		,country
		,round(avg(max_temp_c),2) as max_temp_c
		,round(avg(min_temp_c),2) as min_temp_c
		,round(avg(avg_temp_c),2) as avg_temp_c 
		,lat		--- 2
        ,lon		--- 2
        ,timezone_id--- 2
		,SUM(CASE WHEN condition_text = 'Sunny' THEN 1 ELSE 0 END) AS sunny_days
    	,SUM(CASE WHEN condition_text IN 
                    ('Overcast', 'Partly cloudy', 'Cloudy', 'Freezing fog') 
                    THEN 1 ELSE 0 END) AS other_days
    	,SUM(CASE WHEN condition_text IN 
                    ('Patchy rain possible','Moderate or heavy rain shower', 'Light rain shower',
                    'Mist', 'Moderate rain at times', 'Patchy light rain with thunder',
                    'Patchy light drizzle', 'Thundery outbreaks possible', 'Heavy rain at times', 
                    'Fog', 'Moderate or heavy rain with thunder',  'Light drizzle', 'Light rain', 
                    'Patchy light rain', 'Heavy rain', 'Moderate rain', 'Torrential rain shower', 
                    'Light snow showers', 'Moderate or heavy snow showers', 'Light freezing rain',
                    'Moderate or heavy freezing rain', 'Heavy freezing drizzle') 
                    THEN 1 ELSE 0 END) AS rainy_days
    	,SUM(CASE WHEN condition_text IN 
                    ('Patchy light snow', 'Heavy snow', 'Light sleet', 'Light snow', 
                    'Moderate snow', 'Light sleet showers', 'Patchy heavy snow',
                    'Patchy moderate snow', 'Moderate or heavy snow with thunder',
                    'Moderate or heavy sleet', 'Blizzard', 'Blowing snow', 'Patchy snow possible', 
                    'Moderate or heavy showers of ice pellets', 'Patchy light snow with thunder',
                    'Patchy sleet possible', 'Ice pellets') 
                    THEN 1 ELSE 0 END) AS snowy_days
from {{ref('mart_forecast_day')}} 
group by date
	,year
	,month_of_year
	,month      --- group on
	,city
	,country	--- group on that too
	,lat		--- 2
    ,lon		--- 2
    ,timezone_id--- 2
order by city asc    -- optional
	,year asc
	,month asc     --- order by month
)
SELECT * 
FROM filtering_features
