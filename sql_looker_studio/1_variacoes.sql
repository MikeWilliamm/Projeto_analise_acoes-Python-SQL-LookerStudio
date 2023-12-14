WITH
variacao_five_days as(
  --Variação 5 dias
    with
    inicial_and_final_date as(
      with
      generate_rank_date as (
        select r.ticker, r.date_reference, rank() over (partition by r.ticker order by r.date_reference desc) as rank_date from (
          select o.ticker, CAST(o.date AS DATE) AS date_reference
          from `acoes-378306.acoes.acoes_history` as o
          where cast(o.date as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) 
          group by 1,2
        ) as r
      )
      select grd.ticker, min(grd.date_reference) as inicial_date, max(grd.date_reference) as final_date
      from generate_rank_date as grd
      where rank_date <= 5
      group by 1
    )
    ,value_inicial_date as ( 
      select o.ticker, round(o.close, 2) as close_value_inicial, o.date
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vi on vi.ticker = o.ticker and vi.inicial_date = cast(o.date as date)
    )
    ,value_final_date as (
      select o.ticker, round(o.close, 2) as close_value_final
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vf on vf.ticker = o.ticker and vf.final_date = cast(o.date as date)
    )
    ,calc_variacao as (
      select 
        vi.ticker,         case when vf.close_value_final = 0 and vi.close_value_inicial = 0 and vi.close_value_inicial = 0
        then null 
        else round(((vf.close_value_final - vi.close_value_inicial)/vi.close_value_inicial)*100, 2) end as variacao
      from value_inicial_date as vi
      inner join value_final_date as vf on vf.ticker = vi.ticker
    )
    select
      '5 Dias' as filter_type,
      cv.ticker, 
      case when cv.variacao < 0 then FORMAT('%.2f',cv.variacao) || '%' else '+' || FORMAT('%.2f',cv.variacao) || '%' end as variacao_fomated
    from calc_variacao as cv
)
,variacao_one_month as (
  --Variação 1 mês
    with
    inicial_and_final_date as(
      select o.ticker, cast(min(o.date) as date) as inicial_date, cast(max(o.date) as date) as final_date
      from `acoes-378306.acoes.acoes_history` as o
      where cast(o.date as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) 
      group by 1
    )
    ,value_inicial_date as ( 
      select o.ticker, round(o.close, 2) as close_value_inicial
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vi on vi.ticker = o.ticker and vi.inicial_date = cast(o.date as date) 
    )
    ,value_final_date as (
      select o.ticker, round(o.close, 2) as close_value_final
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vf on vf.ticker = o.ticker and vf.final_date = cast(o.date as date)
    )
    ,calc_variacao as (
      select 
        vi.ticker,         case when vf.close_value_final = 0 and vi.close_value_inicial = 0 and vi.close_value_inicial = 0
        then null 
        else round(((vf.close_value_final - vi.close_value_inicial)/vi.close_value_inicial)*100, 2) end as variacao
      from value_inicial_date as vi
      inner join value_final_date as vf on vf.ticker = vi.ticker
    )
    select
      '1 Mês' as filter_type,
      cv.ticker, 
      case when cv.variacao < 0 then FORMAT('%.2f',cv.variacao) || '%' else '+' || FORMAT('%.2f',cv.variacao) || '%' end as variacao_fomated
    from calc_variacao as cv
)
,variacao_six_months as (
    --6 MESES
      with
    inicial_and_final_date as(
      select o.ticker, cast(min(o.date) as date) as inicial_date, cast(max(o.date) as date) as final_date
      from `acoes-378306.acoes.acoes_history` as o
      where cast(o.date as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
      group by 1
    )
    ,value_inicial_date as ( 
      select o.ticker, round(o.close, 2) as close_value_inicial
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vi on vi.ticker = o.ticker and vi.inicial_date = cast(o.date as date)
    )
    ,value_final_date as (
      select o.ticker, round(o.close, 2) as close_value_final
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vf on vf.ticker = o.ticker and vf.final_date = cast(o.date as date)
    )
    ,calc_variacao as (
      select 
        vi.ticker, 
        case when vf.close_value_final = 0 and vi.close_value_inicial = 0 and vi.close_value_inicial = 0
        then null 
        else round(((vf.close_value_final - vi.close_value_inicial)/vi.close_value_inicial)*100, 2) end as variacao
      from value_inicial_date as vi
      inner join value_final_date as vf on vf.ticker = vi.ticker
    )
    select
      '6 Mês' as filter_type,
      cv.ticker, 
      case when cv.variacao < 0 then FORMAT('%.2f',cv.variacao) || '%' else '+' || FORMAT('%.2f',cv.variacao) || '%' end as variacao_fomated
    from calc_variacao as cv
)
,varicao_ytd as (
  --Variação YTD
    with
    inicial_and_final_date as(
      select o.ticker, cast(min(o.date) as date) as inicial_date, cast(max(o.date) as date) as final_date
      from `acoes-378306.acoes.acoes_history` as o
      where cast(o.date as date) >= DATE_TRUNC(CURRENT_DATE(), YEAR)
      group by 1

    )
    ,value_inicial_date as ( 
      select o.ticker, round(o.close, 2) as close_value_inicial
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vi on vi.ticker = o.ticker and vi.inicial_date = cast(o.date as date)
    )
    ,value_final_date as (
      select o.ticker, round(o.close, 2) as close_value_final
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vf on vf.ticker = o.ticker and vf.final_date = cast(o.date as date)
    )
    ,calc_variacao as (
      select 
        vi.ticker,         case when vf.close_value_final = 0 and vi.close_value_inicial = 0 and vi.close_value_inicial = 0
        then null 
        else round(((vf.close_value_final - vi.close_value_inicial)/vi.close_value_inicial)*100, 2) end as variacao
      from value_inicial_date as vi
      inner join value_final_date as vf on vf.ticker = vi.ticker
    )
    select
      'YTD' as filter_type,
      cv.ticker, 
      case when cv.variacao < 0 then FORMAT('%.2f',cv.variacao) || '%' else '+' || FORMAT('%.2f',cv.variacao) || '%' end as variacao_fomated
    from calc_variacao as cv
)
,variacao_one_year as (
  --Variação 1 ANO
    with
    inicial_and_final_date as(
      select o.ticker, cast(min(o.date) as date) as inicial_date, cast(max(o.date) as date) as final_date
      from `acoes-378306.acoes.acoes_history` as o
      where cast(o.date as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
      group by 1
    )
    ,value_inicial_date as ( 
      select o.ticker, round(o.close, 2) as close_value_inicial
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vi on vi.ticker = o.ticker and vi.inicial_date = cast(o.date as date)
    )
    ,value_final_date as (
      select o.ticker, round(o.close, 2) as close_value_final
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vf on vf.ticker = o.ticker and vf.final_date = cast(o.date as date)
    )
    ,calc_variacao as (
      select 
        vi.ticker,         case when vf.close_value_final = 0 and vi.close_value_inicial = 0 and vi.close_value_inicial = 0
        then null 
        else round(((vf.close_value_final - vi.close_value_inicial)/vi.close_value_inicial)*100, 2) end as variacao
      from value_inicial_date as vi
      inner join value_final_date as vf on vf.ticker = vi.ticker
    )
    select
      '1 Ano' as filter_type,
      cv.ticker, 
      case when cv.variacao < 0 then FORMAT('%.2f',cv.variacao) || '%' else '+' || FORMAT('%.2f',cv.variacao) || '%' end as variacao_fomated
    from calc_variacao as cv
)
,varicao_five_years as (
  --Variação 5 ANOs
    with
    inicial_and_final_date as(
      select o.ticker, cast(min(o.date) as date) as inicial_date, cast(max(o.date) as date) as final_date
      from `acoes-378306.acoes.acoes_history` as o
      where cast(o.date as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
      group by 1
    )
    ,value_inicial_date as ( 
      select o.ticker, round(o.close, 2) as close_value_inicial
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vi on vi.ticker = o.ticker and vi.inicial_date = cast(o.date as date)
    )
    ,value_final_date as (
      select o.ticker, round(o.close, 2) as close_value_final
      from `acoes-378306.acoes.acoes_history` as o
      inner join inicial_and_final_date as vf on vf.ticker = o.ticker and vf.final_date = cast(o.date as date)
    )
    ,calc_variacao as (
      select 
        vi.ticker,         case when vf.close_value_final = 0 and vi.close_value_inicial = 0 and vi.close_value_inicial = 0
        then null 
        else round(((vf.close_value_final - vi.close_value_inicial)/vi.close_value_inicial)*100, 2) end as variacao
      from value_inicial_date as vi
      inner join value_final_date as vf on vf.ticker = vi.ticker
    )
    select
      '5 Anos' as filter_type,
      cv.ticker, 
      case when cv.variacao < 0 then FORMAT('%.2f',cv.variacao) || '%' else '+' || FORMAT('%.2f',cv.variacao) || '%' end as variacao_fomated
    from calc_variacao as cv
)
,distinct_ticker_atual_value as (
    with
    get_final_date as (
      select o.ticker, CAST(max(o.date) as DATE) as final_date
      from `acoes-378306.acoes.acoes_history` as o
      group by 1
    )
    select distinct o.ticker, o.close as price from `acoes-378306.acoes.acoes_history` AS o
    inner join get_final_date as gt on gt.ticker = o.ticker and gt.final_date = cast(o.date as date)
)

SELECT o.ticker, o.price, fd.variacao_fomated as porcent_variacao_five_days, om.variacao_fomated as porcent_variacao_one_month,
       sm.variacao_fomated as porcent_variacao_six_months, ytd.variacao_fomated as porcent_varicao_ytd,
       oy.variacao_fomated as porcent_variacao_one_year, fy.variacao_fomated as porcent_varicao_five_years,
       o.ticker || '    -    ' || da.companyname as empresa_ticker
from distinct_ticker_atual_value AS o
left join variacao_five_days as fd on fd.ticker = o.ticker
left join variacao_one_month as om on om.ticker = o.ticker
left join variacao_six_months as sm on sm.ticker = o.ticker
left join varicao_ytd as ytd on ytd.ticker = o.ticker
left join variacao_one_year as oy on oy.ticker = o.ticker
left join varicao_five_years as fy on fy.ticker = o.ticker
left join `acoes-378306.acoes.data_acoes` as da on da.ticker = o.ticker