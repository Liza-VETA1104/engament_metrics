with dau as(
	select 
		session_start::date as day, 		
		count(distinct user_id) user_cnt
from user_sessions us  
group by us.session_start::date
),
avg_dau as (
	select
		to_char(day, 'YYYY-MM') as months,
		avg(user_cnt) as dau
from dau
group by to_char(day, 'YYYY-MM')
	),
mau as(
	select 
		to_char(session_start, 'YYYY-MM') as months, 
		count(distinct user_id) as mau
from user_sessions
group by to_char(session_start, 'YYYY-MM')
)
select
	m.months,
	round(dau / mau, 2) as sticky_factor
from mau m
join avg_dau as a on
	a.months = m.months
order by m.months
