WITH
inicial_and_final_date_five_days as(
  with
  generate_rank_date as (
    select r.ticker, r.date_reference, rank() over (partition by r.ticker order by r.date_reference desc) as rank_date from (
      select o.ticker, CAST(o.date AS DATE) AS date_reference
      from `acoes-378306.acoes.acoes_history` as o
      where cast(o.date as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY) 
      group by 1,2
    ) as r
  )
  select grd.ticker, grd.date_reference as date_reference
  from generate_rank_date as grd
  where rank_date <= 5
)
,union_data as (
  select '5 Dias' as filter_type, o.ticker, cast(o.date as date) as date, round(o.close, 2) as close_value
  from `acoes-378306.acoes.acoes_history` as o
  inner join inicial_and_final_date_five_days as fd on fd.ticker = o.ticker and fd.date_reference = cast(o.date as date)
  union all
  select '1 Mês' as filter_type, o.ticker, cast(o.date as date) as date, round(o.close, 2) as close_value
  from `acoes-378306.acoes.acoes_history` as o
  where cast(o.date as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
  union all
  select '6 Mêses' as filter_type, o.ticker, cast(o.date as date) as date, round(o.close, 2) as close_value
  from `acoes-378306.acoes.acoes_history` as o
  where cast(o.date as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
  union all
  select 'YTD' as filter_type, o.ticker, cast(o.date as date) as date, round(o.close, 2) as close_value
  from `acoes-378306.acoes.acoes_history` as o
  where cast(o.date as date) >= DATE_TRUNC(CURRENT_DATE(), YEAR)
  union all
  select '1 Ano' as filter_type, o.ticker, cast(o.date as date) as date, round(o.close, 2) as close_value
  from `acoes-378306.acoes.acoes_history` as o
  where cast(o.date as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  union all
  select '5 Anos' as filter_type, o.ticker, cast(o.date as date) as date, round(o.close, 2) as close_value
  from `acoes-378306.acoes.acoes_history` as o
  where cast(o.date as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
)
select un.filter_type, un.ticker, un.date, un.close_value,un.ticker || '    -    ' || da.companyname as empresa_ticker
from union_data as un 
inner join `acoes-378306.acoes.data_acoes` as da on da.ticker = un.ticker
