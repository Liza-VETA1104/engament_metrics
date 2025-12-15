with user_primary_device as (  --выбираем приоритетное устройство по количеству использований и времени использования
    select 
        user_id,
        device_type,
        row_number() over (
            partition by user_id 
            order by count(*) desc, max(session_start) desc, device_type
        ) as device_rank
    from user_sessions
    group by user_id, device_type
),
primary_device as (
    select 
        user_id,
        device_type as primary_device
    from user_primary_device
    where device_rank = 1
),
user_behavior as (
    select 
        user_id,
        avg(pages_viewed) as avg_pages_per_session,
        avg(session_duration_minutes) as avg_session_minutes,
        count(distinct session_id) as total_sessions
    from user_sessions
    group by user_id
),
user_orders as (
    select 
        user_id,
        count(distinct order_id) as orders_count,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_order_value
    from orders
    group by user_id
),
user_summary as (
    select 
        pd.primary_device as device_type,
        ub.user_id,
        ub.avg_pages_per_session,
        ub.avg_session_minutes,
        ub.total_sessions,
        coalesce(uo.orders_count, 0) as orders_count,
        coalesce(uo.total_revenue, 0) as total_revenue,
        coalesce(uo.avg_order_value, 0) as avg_order_value
    from user_behavior ub
    join primary_device pd on ub.user_id = pd.user_id
    left join user_orders uo on ub.user_id = uo.user_id
)
select 
    device_type,
    round(100.0 * count(distinct user_id) / sum(count(distinct user_id)) over(), 2) as device_share_percent,
    count(distinct user_id) as users_count,
    round(avg(avg_pages_per_session), 2) as avg_pages_per_session,
    round(avg(avg_session_minutes), 2) as avg_session_minutes,
    round(avg(total_sessions), 2) as avg_sessions_per_user,
    round(100.0 * sum(case when orders_count > 0 then 1 else 0 end) / count(distinct user_id), 2) as conversion_rate_percent,
    round(avg(orders_count), 2) as avg_orders_per_user,
    round(avg(total_revenue), 2) as avg_revenue_per_user,
    round(sum(total_revenue) / nullif(sum(orders_count), 0), 2) as avg_order_value
from user_summary
group by device_type
order by users_count desc
