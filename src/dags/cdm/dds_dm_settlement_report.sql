select 
dmo.restaurant_id,
restaurant_name,
date as settlement_date,
count(distinct order_id) as orders_count,
sum(total_sum) as orders_total_sum,
sum(bonus_payment) as orders_bonus_payment_sum,
sum(bonus_grant) as orders_bonus_granted_sum,
0.25*sum(total_sum) as order_processing_fee,
0.75*sum(total_sum)-sum(bonus_payment) as restaurant_reward_sum
from dds.fct_product_sales fps left join dds.dm_orders dmo
on fps.order_id=dmo.id
left join dds.dm_timestamps dmt
on dmo.timestamp_id=dmt.id
left join dds.dm_restaurants dmr
on dmo.restaurant_id=dmr.id
where dmo.order_status='CLOSED'
group by dmo.restaurant_id, restaurant_name, date;