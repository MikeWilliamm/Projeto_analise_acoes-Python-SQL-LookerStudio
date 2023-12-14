with
min_max_year as (
  SELECT dy.ticker, min(dy.created_at) as min_year, extract(year from current_date()) as max_year
  FROM `acoes-378306.acoes.acoes_porcent_dy` as dy
  group by 1
),
ticker_year as (
  SELECT
    mmy.ticker,
    year
  FROM
    min_max_year AS mmy
  CROSS JOIN
    UNNEST(GENERATE_ARRAY(mmy.min_year, mmy.max_year)) AS year
)
,final_all_data as (
select ty.ticker, da.ticker || '    -    ' || da.companyname as empresa_ticker,ty.year,
       case when dy.created_at is null then 0 else dy.price end as price_porcent,
       case when dyv.created_at is null then 0 else dyv.price end as price_valor,
       'HISTÓRICO COMPLETO' as filter_type_dy
from ticker_year as ty
left join `acoes-378306.acoes.acoes_porcent_dy` as dy on ty.year = dy.created_at and ty.ticker = dy.ticker
left join `acoes-378306.acoes.acoes_valor_dy` as dyv on ty.year = dyv.created_at and ty.ticker = dyv.ticker
inner join `acoes-378306.acoes.data_acoes` as da on da.ticker = ty.ticker
)
,final_five_year_data as (
SELECT ticker, empresa_ticker,year, price_porcent, price_valor, 'HISTÓRICO ÚLTIMOS 5 ANOS' as filter_type_dy FROM final_all_data
where year >= extract(year from current_date()) - 4
)
select * from final_all_data
union all
select * from final_five_year_data