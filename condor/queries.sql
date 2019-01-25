# macro_combined BigQuery view
select
  *
from
(
  select
    date(inx.date) as date,
    inx.inx_close_value as inx_close_value,
    effr.effr_rate as effr_rate,
    gold.gold_price_usd as gold_price_usd
  from
    [peppy-bond-105017:macroeconomic.inx] as inx
  left join
    [peppy-bond-105017:macroeconomic.effr] as effr
    on inx.date = effr.date
  left join
    [peppy-bond-105017:macroeconomic.gold] as gold
    on inx.date = gold.date
)
where
  inx_close_value is not null
  and effr_rate is not null
  and gold_price_usd is not null
order by
  1 desc
