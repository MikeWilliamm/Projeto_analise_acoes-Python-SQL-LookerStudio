with
calc_valor_justo as (
  select gv.ticker, dc.ticker || '    -    ' || dc.companyname as empresa_ticker, gv.close as valor_atual,
        CASE 
        WHEN dc.lpa > 0 and dc.vpa > 0 and dc.liquidezmediadiaria > 1 and dc.p_vp > 0 and dc.p_l > 0 and sectorname !='Tecnologia da Informação'
        then round(SQRT(22.5 * dc.lpa * dc.vpa),2) else null end as valor_intrinseco
  from `acoes-378306.acoes.acoes_metrica_atual` as gv
  inner join `acoes-378306.acoes.data_acoes` as dc on gv.ticker = dc.ticker
)
,calc_percentual as (
  select cv.ticker, cv.empresa_ticker, cv.valor_atual, cv.valor_intrinseco,
        round(((cv.valor_intrinseco - cv.valor_atual)/cv.valor_atual)*100, 2) as percentual_valorizacao
  from calc_valor_justo as cv
)
select cp.ticker, cp.empresa_ticker, FORMAT('%0.2f', cp.valor_atual) as valor_atual,
       case when cp.valor_intrinseco is null then 'Fundamento não se aplica!' else 'R$ ' || FORMAT('%0.2f', cp.valor_intrinseco) end as valor_intrinseco,
       case 
        when cp.percentual_valorizacao is null then 'Fundamento não se aplica!' 
        when cp.percentual_valorizacao > 0 then '+' || FORMAT('%0.2f', cp.percentual_valorizacao) || '%'
        else FORMAT('%0.2f', cp.percentual_valorizacao) || '%'
        end as percentual_valorizacao,
        rank() over (order by cp.percentual_valorizacao DESC) as rank_valorizacao
from calc_percentual as cp
order by rank_valorizacao