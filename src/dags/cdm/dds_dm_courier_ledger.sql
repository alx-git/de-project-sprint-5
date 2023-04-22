select 
dmc.courier_id,
dmc.courier_name,
dmt.year as settlement_year,
dmt.month as settlement_month,
count(dmo.id) as orders_count,
sum(fps.total_sum) as orders_total_sum,
avg(dmd.rate) as rate_avg,
sum(fps.total_sum)*0.25 as order_processing_fee,
case when avg(dmd.rate) < 4 then (case when 0.05*sum(fps.total_sum) < 100 then 100 else 0.05*sum(fps.total_sum) end) when avg(dmd.rate) < 4.5 then (case when 0.07*sum(fps.total_sum) < 150 then 150 else 0.07*sum(fps.total_sum) end) when avg(dmd.rate) < 4.9 then (case when 0.08*sum(fps.total_sum) < 175 then 175 else 0.08*sum(fps.total_sum) end) else (case when 0.1*sum(fps.total_sum) < 200 then 200 else 0.1*sum(fps.total_sum) end) end as courier_order_sum,
sum(dmd.tip_sum) as courier_tips_sum,
(case when avg(dmd.rate) < 4 then (case when 0.05*sum(fps.total_sum) < 100 then 100 else 0.05*sum(fps.total_sum) end) when avg(dmd.rate) < 4.5 then (case when 0.07*sum(fps.total_sum) < 150 then 150 else 0.07*sum(fps.total_sum) end) when avg(dmd.rate) < 4.9 then (case when 0.08*sum(fps.total_sum) < 175 then 175 else 0.08*sum(fps.total_sum) end) else (case when 0.1*sum(fps.total_sum) < 200 then 200 else 0.1*sum(fps.total_sum) end) end + sum(dmd.tip_sum)*0.95) as courier_reward_sum
from dds.dm_couriers dmc left join dds.dm_deliveries dmd
on dmc.id=dmd.courier_id 
left join dds.dm_orders dmo
on dmd.order_id=dmo.id
left join dds.dm_timestamps dmt
on dmo.timestamp_id=dmt.id
left join dds.fct_product_sales fps
on dmo.id=fps.order_id
where dmo.order_status='CLOSED'
group by dmc.courier_id, dmc.courier_name, dmt.year, dmt.month;