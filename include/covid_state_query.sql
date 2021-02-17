with yesterday_covid_data as (
    SELECT *
    FROM covid_state_data
    WHERE date = {{ yesterday_ds_nodash }}
    AND state = {{ params.state }}
),
today_covid_data as (
    SELECT *
    FROM covid_state_data
    WHERE date = {{ ds_nodash }}
    AND state = {{ params.state }}
),
two_day_rolling_avg as (
    SELECT AVG(a.state, b.state) as two_day_avg
    FROM yesterday_covid_data a
    JOIN yesterday_covid_data b 
    ON a.state = b.state
)
SELECT a.state, b.state, c.two_day_avg
FROM yesterday_covid_data a
JOIN today_covid_data b
ON a.state=b.state
JOIN two_day_rolling_avg c
ON a.state=b.two_day_avg;
