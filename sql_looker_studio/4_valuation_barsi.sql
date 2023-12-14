with
ticker_with_5_year_history as (--acoes com historico de dividendos maior que 5 anos
  SELECT dy.ticker, max(dy.created_at) - min(dy.created_at) as max_year
  FROM `acoes-378306.acoes.acoes_valor_dy` as dy
  group by 1
  having (max(dy.created_at) - min(dy.created_at)) >= 5
)
,ticker_not_have_5_year_history as (--acoes com historico de dividendos menor que 5 anos
  SELECT dy.ticker, max(dy.created_at) - min(dy.created_at) as max_year
  FROM `acoes-378306.acoes.acoes_valor_dy` as dy
  group by 1
  having (max(dy.created_at) - min(dy.created_at)) < 5
)
,min_max_year as (--de forma forçada seta o ano atual e 5 anos retroativos
  SELECT dy.ticker, extract(year from current_date()) - 4 as min_year,  extract(year from current_date()) as max_year
  FROM `acoes-378306.acoes.acoes_valor_dy` as dy
  inner join ticker_with_5_year_history as tw3 on tw3.ticker = dy.ticker
  group by 1
)
,ticker_year as (--gera array dos 5 anos retroativos para cada ticker
  SELECT
    mmy.ticker,
    year
  FROM
    min_max_year AS mmy
  CROSS JOIN
    UNNEST(GENERATE_ARRAY(mmy.min_year, mmy.max_year)) AS year
)
,get_dividend_last_five_years as (--pega dividendos que existiu em cada ano
  select ty.ticker, da.ticker || '    -    ' || da.companyname as empresa_ticker,ty.year,
        case when dy.created_at is null then 0 else dy.price end as price
  from ticker_year as ty
  left join `acoes-378306.acoes.acoes_valor_dy` as dy on ty.year = dy.created_at and ty.ticker = dy.ticker
  inner join `acoes-378306.acoes.data_acoes` as da on da.ticker = ty.ticker
  WHERE da.price > 0 and da.sectorname is not null
  order by ty.ticker, ty.year
)
,calc_preco_teto as (--gera preço teto
  select gd.ticker, gd.empresa_ticker,
          gv.close as valor_atual,
          FORMAT('%0.2f', ROUND((sum(gd.price)/5) / (@porcent/100), 2)) as preco_teto,
          case when sum(gd.price) > 0 and gv.close > 0 
          then FORMAT('%0.2f', round(((((sum(gd.price)/5) / (@porcent/100)) - gv.close)/gv.close)*100, 2))
          else '0' end as percent
    from get_dividend_last_five_years as gd
    inner join `acoes-378306.acoes.acoes_metrica_atual` as gv on gv.ticker = gd.ticker
    group by 1,2,3
)
,union_data as (--faz union entre
                -- açoes com historico de 5 anos de dividendos e com calculo de preço teto apicado
                --açoes com menos de 5 nos de dividendos, mas sem preço teto, pois fundamento não se aplica
  select gd.ticker, gd.empresa_ticker,
        gd.valor_atual,
        'R$ ' || preco_teto as preco_teto,
        case when cast(percent as float64) >= 0 
        then '+' || percent || '%'
        else percent || '%'
        end as percentual_valorizacao,
        RANK() OVER (order by cast(percent as float64) desc) as rank_valorizacao
  from calc_preco_teto as gd
  inner join `acoes-378306.acoes.acoes_metrica_atual` as gv on gv.ticker = gd.ticker
  UNION ALL
  SELECT tn.ticker, da.ticker || '    -    ' || da.companyname as empresa_ticker,
        gv.close as valor_atual, 'Fundamento não se aplica!' as preco_teto,
        'Fundamento não se aplica!' as percentual_valorizacao,999999 as rank_valorizacao
  FROM ticker_not_have_5_year_history as tn
  inner join `acoes-378306.acoes.data_acoes` as da on da.ticker = tn.ticker
  inner join `acoes-378306.acoes.acoes_metrica_atual` as gv on gv.ticker = tn.ticker
)
select ticker, empresa_ticker, FORMAT('%0.2f',valor_atual) as valor_atual, 
       preco_teto, percentual_valorizacao,
       rank_valorizacao
from union_data
--where ticker = 'BBAS3'
order by rank_valorizacao