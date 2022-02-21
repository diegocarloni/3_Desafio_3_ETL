-- ETL Pentaho para MySQL

show databases;
use desafio3;
show tables;
desc space_tratado2;
select * from space_tratado2 limit 15;
drop table space_tratado2;

desc space_tratado2;

-- QUERYS

-- Quantos registros existem?
select count(id_lancamento) as "Quantidade_registros"  from space_tratado2;

-- Quantas missões/projetos não foram divulgados seus respectivos custos?
select count(id_lancamento) as "custos_nao_informados" from space_tratado2
where custo_projeto = "" or custo_projeto is null;

-- Quantas missões tiveram Sucesso?
select distinct status_missao from space_tratado2;

select count(status_missao) as "Missoes_bem_sucedidas" 
from space_tratado2
where status_missao like "Sucesso";

-- Qual a porcentagem de missoes bem sucedidas?
select sum(case when status_missao = "Sucesso" then 1 else 0 end) as "Missoes_bem_sucedidas",
count(id_lancamento) as "total_missoes",
truncate(((sum(case when status_missao = "Sucesso" then 1 else 0 end))/(count(id_lancamento))*100),2) as Porcentagem_missoes_sucesso
from space_tratado2;

-- Quantas missões falharam?
select count(status_missao) as "Missoes_mal_sucedidas" 
from space_tratado2
where status_missao like "Falha%";

-- Qual a porcentagem de missoes mal sucedidas?
select sum(case when status_missao like "Falha%" then 1 else 0 end) as "Missoes_mal_sucedidas",
count(id_lancamento) as "total_missoes",
truncate(((sum(case when status_missao like "Falha%" then 1 else 0 end))/(count(id_lancamento))*100),2) as Porcentagem_missoes_mal_sucedidas
from space_tratado2;

select count(status_missao) as "Missoes_mal_sucedidas" 
from space_tratado2
where status_missao = "Falha" or status_missao = "Falha_pre_lancamento" or status_missao = "Falha_parcial";

-- 	Qual dia da semana teve maior numero de missoes bem sucedidas?
select distinct(data_dia_semana) 
from space_tratado2;

select data_dia_semana, count(data_dia_semana) as qdd_missoes 
from space_tratado2
where status_missao = "Sucesso"
group by data_dia_semana
order by (qdd_missoes) desc;

-- 	Qual dia da semana teve maior numero de missoes bem sucedidas?
select data_dia_semana, count(data_dia_semana) as qdd_missoes 
from space_tratado2
where status_missao = "Sucesso"
group by data_dia_semana
order by (qdd_missoes) desc limit 1;

-- 	Qual dia da semana teve menor numero de missoes bem sucedidas?
select data_dia_semana, count(data_dia_semana) as qdd_missoes 
from space_tratado2
where status_missao = "Sucesso"
group by data_dia_semana
order by (qdd_missoes) asc limit 1;

-- 	Dentre os tipos das falhas, qual teve a maior ocorrência?
select 
sum(case when status_missao = "Falha" then 1 else 0 end) as qtdd_falha, 
sum(case when status_missao = "Falha_parcial" then 1 else 0 end) as qtdd_falha_parcial,
sum(case when status_missao = "Falha_pre_lancamento" then 1 else 0 end) as qtdd_Falha_pre_lancamento
from space_tratado2; 

-- Com base nos registros, quantos modelos diferentes de foguetes foram utilizados?
select count(distinct(modelo_foguete)) as qtt_foguetes_total
from space_tratado2
where foguete_status = "Ativo" or foguete_status = "Inativo";

-- Quantos modelos ainda estão ativos?
select count(distinct(modelo_foguete)) as qtt_foguete_ativos
from space_tratado2
where foguete_status = "Ativo";

-- Quantos modelos foram aposentados?
select count(distinct(modelo_foguete)) as qtt_foguete_desativados
from space_tratado2
where foguete_status = "Inativo";

-- Quais modelos de foguete ainda estão ativos? 
select distinct(modelo_foguete), foguete_status
from space_tratado2
where foguete_status = "Ativo";

-- Dos modelos ainda ativos, quais foram os 3 que mais participaram de missoes bem sucedidas? quantas missoes?
select modelo_foguete, count(modelo_foguete) as qtt_missoes_foguetes_ativos
from space_tratado2
where foguete_status = "Ativo" and status_missao = "Sucesso"
group by modelo_foguete
order by qtt_missoes_foguetes_ativos desc limit 3;

-- Dos modelos desativados, quais foram os 3 que mais participaram de missoes bem sucedidas? quantas missoes?
select modelo_foguete, count(modelo_foguete) as qtt_missoes_foguetes_inativos
from space_tratado2
where foguete_status = "Inativo" and status_missao = "Sucesso"
group by modelo_foguete
order by qtt_missoes_foguetes_inativos desc limit 3;

-- Qual foi o foguete que participou de mais missoes, bem sucedidas ou não, (dentre os ativos e inativos)?
select modelo_foguete, count(modelo_foguete) as qtt_missoes_totais
from space_tratado2
where foguete_status = "Inativo" or foguete_status = "Ativo" 
group by modelo_foguete
order by qtt_missoes_totais desc limit 1;

select modelo_foguete, count(modelo_foguete) as Sucesso
from space_tratado2
where modelo_foguete =  "Cosmos-3M (11K65M)" and status_missao = "Sucesso";

select modelo_foguete, count(modelo_foguete) as Falhas
from space_tratado2
where modelo_foguete =  "Cosmos-3M (11K65M)" and status_missao like "Falha%";

-- Quais missoes/projetos não tem o registro do pais?
select distinct pais as "pais_nao_informado", id_lancamento, nome_empresa 
from space_tratado2
where pais is null or pais = "";

-- Qual a quantidade de países que tem pelo menos um registro de missao? (Independente se a missao
-- foi bem sucedida ou não)
select count(distinct(pais))
from space_tratado2
where pais is not null or pais != "";

-- Quais são esses países?
select distinct pais  
from space_tratado2
where pais is not null or pais = "";

-- Quais são os top 3 países que mais realizaram missoes, independente se teve sucesso ou falha?
select pais, count(status_missao) as qtt_missoes_t
from space_tratado2
group by pais
order by qtt_missoes_t desc limit 3;

-- Quais foram os paises que realizaram pelo menos uma missao com sucesso?
select pais, count(status_missao) as qtt_missoes_sucesso
from space_tratado2
where status_missao = "Sucesso" and pais is not null
group by pais
order by qtt_missoes_sucesso;

-- Quais foram os paises que realizaram pelo menos uma missao com qualquer tipo de falha?
select pais, count(status_missao) as qtt_missoes_falha
from space_tratado2
where status_missao like "Falha%" and pais is not null
group by pais
order by qtt_missoes_falha;

-- Quais foram os paises que todas as missoes realizadas falharam?
select pais, sum(case when status_missao like "Falha%" then 1 else 0 end) as qdd_falhas,
count(id_lancamento) as total 
from space_tratado2
where pais is not null
group by pais
having qdd_falhas = total;

-- Quais foram os paises que todas as missoes realizadas tiveram sucesso?
select pais, sum(case when status_missao like "Sucesso" then 1 else 0 end) as qdd_sucesso,
count(id_lancamento) as total 
from space_tratado2
where pais is not null
group by pais
having qdd_sucesso = total;

-- Baseado no histórico, quais os top 5 anos que mais missoes foram realizadas?
select data_ano as ano, count(data_ano) as qtt_m_ano 
from space_tratado2
group by ano
order by qtt_m_ano desc limit 5;  

-- Baseado no histórico, quais foram os 3 meses que mais missoes foram realizadas?
select data_mes as mes, count(data_mes) as qtt_m_mes 
from space_tratado2
group by mes
order by qtt_m_mes desc limit 3;

-- Qual horário teve mais lancamentos? quantos lancamentos? Mostre o ranking dos top 15 horários preferidos.
select data_hora as hora,
count(data_hora) as qdd
from space_tratado2
group by data_hora
order by qdd desc limit 15;

-- Qual foi a data do primeiro lançamento, segundo a base? qual foi o pais, modelo do foguete e objetivo?
select junta_data_hora as data_primeiro_lancamento, pais, modelo_foguete, objetivo_missao
from space_tratado2
where junta_data_hora = (select min(junta_data_hora) from space_tratado2);

-- Qual foi a data do ultimo lançamento, segundo a base? qual foi o pais, modelo do foguete e objetivo?
select junta_data_hora as data_lancamento_recente, pais, modelo_foguete, objetivo_missao
from space_tratado2
where junta_data_hora = (select max(junta_data_hora) from space_tratado2);

-- qual o valor medio do custo entre os lancamentos?
select truncate(avg(custo_projeto),2) as media_custo_projeto
from space_tratado2
where custo_projeto is not null;

-- quais paises gastaram ou tiveram custos com lancamento acima da media?
select pais, custo_projeto as custo_acima_media from space_tratado2
where custo_projeto > (select avg(custo_projeto) from space_tratado2)
group by pais;

-- Quantos lancamentos aconteceram a partir do periodo de 01/01/2020 até o dia de hoje?
select 
count(*) as qdd_periodo
from space_tratado2
where junta_data_hora between '2020-01-01 00:00:00' and now();

select 
count(*) from space_tratado2
where data_completa between '2020-01-01' and substr(now(),1,10);

select now();

-- ETL Jupyter notebook (Python) para MySQL

show databases;
use desafio3_py;

show tables;
desc space_tratado;
select * from space_tratado limit 15;
drop table space_tratado;

select distinct(pais)
from space_tratado
where pais is not null and pais != "";

UPDATE space_tratado SET pais = ""
WHERE id_lancamento = 35;

UPDATE space_tratado SET pais = ""
WHERE id_lancamento = 133;

UPDATE space_tratado SET pais = ""
WHERE id_lancamento = 481;

UPDATE space_tratado SET pais = ""
WHERE id_lancamento = 920;

UPDATE space_tratado SET pais = ""
WHERE id_lancamento = 957;

UPDATE space_tratado SET pais = ""
WHERE id_lancamento = 1304;

select * from space_tratado where pais = "";
select * from space_tratado where pais = "" or pais is null;

-- QUERYS

-- Quantos registros existem?
select count(id_lancamento) as "Quantidade_registros"  from space_tratado;

-- Quantas missões/projetos não foram divulgados seus respectivos custos?
select count(id_lancamento) as "custos_nao_informados" from space_tratado
where custo_projeto = "" or custo_projeto is null;

-- Quantas missões tiveram Sucesso?
select distinct status_missao from space_tratado;

select count(status_missao) as "Missoes_bem_sucedidas" 
from space_tratado
where status_missao like "Sucesso";

-- Qual a porcentagem de missoes bem sucedidas?
select sum(case when status_missao = "Sucesso" then 1 else 0 end) as "Missoes_bem_sucedidas",
count(id_lancamento) as "total_missoes",
truncate(((sum(case when status_missao = "Sucesso" then 1 else 0 end))/(count(id_lancamento))*100),2) as Porcentagem_missoes_sucesso
from space_tratado;

-- Quantas missões falharam?
select count(status_missao) as "Missoes_mal_sucedidas" 
from space_tratado
where status_missao like "Falha%";

-- Qual a porcentagem de missoes mal sucedidas?
select sum(case when status_missao like "Falha%" then 1 else 0 end) as "Missoes_mal_sucedidas",
count(id_lancamento) as "total_missoes",
truncate(((sum(case when status_missao like "Falha%" then 1 else 0 end))/(count(id_lancamento))*100),2) as Porcentagem_missoes_mal_sucedidas
from space_tratado;

select count(status_missao) as "Missoes_mal_sucedidas" 
from space_tratado
where status_missao = "Falha" or status_missao = "Falha_pre_lancamento" or status_missao = "Falha_parcial";

-- 	Qual dia da semana teve maior numero de missoes bem sucedidas?
select distinct(data_dia_semana) 
from space_tratado;

select data_dia_semana, count(data_dia_semana) as qdd_missoes 
from space_tratado
where status_missao = "Sucesso"
group by data_dia_semana
order by (qdd_missoes) desc;

-- 	Qual dia da semana teve maior numero de missoes bem sucedidas?
select data_dia_semana, count(data_dia_semana) as qdd_missoes 
from space_tratado
where status_missao = "Sucesso"
group by data_dia_semana
order by (qdd_missoes) desc limit 1;

-- 	Qual dia da semana teve menor numero de missoes bem sucedidas?
select data_dia_semana, count(data_dia_semana) as qdd_missoes 
from space_tratado
where status_missao = "Sucesso"
group by data_dia_semana
order by (qdd_missoes) asc limit 1;

-- 	Dentre os tipos das falhas, qual teve a maior ocorrência?
select 
sum(case when status_missao = "Falha" then 1 else 0 end) as qtdd_falha, 
sum(case when status_missao = "Falha_parcial" then 1 else 0 end) as qtdd_falha_parcial,
sum(case when status_missao = "Falha_pre_lancamento" then 1 else 0 end) as qtdd_Falha_pre_lancamento
from space_tratado; 

-- Com base nos registros, quantos modelos diferentes de foguetes foram utilizados?
select count(distinct(modelo_foguete)) as qtt_foguetes_total
from space_tratado
where foguete_status = "Ativo" or foguete_status = "Inativo";

-- Quantos modelos ainda estão ativos?
select count(distinct(modelo_foguete)) as qtt_foguete_ativos
from space_tratado
where foguete_status = "Ativo";

-- Quantos modelos foram aposentados?
select count(distinct(modelo_foguete)) as qtt_foguete_desativados
from space_tratado
where foguete_status = "Inativo";

-- Quais modelos de foguete ainda estão ativos? 
select distinct(modelo_foguete), foguete_status
from space_tratado
where foguete_status = "Ativo";

-- Dos modelos ainda ativos, quais foram os 3 que mais participaram de missoes bem sucedidas? quantas missoes?
select modelo_foguete, count(modelo_foguete) as qtt_missoes_foguetes_ativos
from space_tratado
where foguete_status = "Ativo" and status_missao = "Sucesso"
group by modelo_foguete
order by qtt_missoes_foguetes_ativos desc limit 3;

-- Dos modelos desativados, quais foram os 3 que mais participaram de missoes bem sucedidas? quantas missoes?
select modelo_foguete, count(modelo_foguete) as qtt_missoes_foguetes_inativos
from space_tratado
where foguete_status = "Inativo" and status_missao = "Sucesso"
group by modelo_foguete
order by qtt_missoes_foguetes_inativos desc limit 3;

-- Qual foi o foguete que participou de mais missoes, bem sucedidas ou não, (dentre os ativos e inativos)?
select modelo_foguete, count(modelo_foguete) as qtt_missoes_totais
from space_tratado
where foguete_status = "Inativo" or foguete_status = "Ativo" 
group by modelo_foguete
order by qtt_missoes_totais desc limit 1;

select modelo_foguete, count(modelo_foguete) as Sucesso
from space_tratado
where modelo_foguete =  "Cosmos-3M (11K65M)" and status_missao = "Sucesso";

select modelo_foguete, count(modelo_foguete) as Falhas
from space_tratado
where modelo_foguete =  "Cosmos-3M (11K65M)" and status_missao like "Falha%";

-- Quais missoes/projetos não tem o registro do pais?
select distinct pais as "pais_nao_informado", id_lancamento, nome_empresa 
from space_tratado
where pais is null or pais = "";

-- Qual a quantidade de países que tem pelo menos um registro de missao? (Independente se a missao
-- foi bem sucedida ou não)
select count(distinct(pais))
from space_tratado
where pais is not null or pais != "";

-- Quais são esses países?
select distinct pais  
from space_tratado
where pais is not null or pais = "";

-- Quais são os top 3 países que mais realizaram missoes, independente se teve sucesso ou falha?
select pais, count(status_missao) as qtt_missoes_t
from space_tratado
group by pais
order by qtt_missoes_t desc limit 3;

-- Quais foram os paises que realizaram pelo menos uma missao com sucesso?
select pais, count(status_missao) as qtt_missoes_sucesso
from space_tratado
where status_missao = "Sucesso" and pais is not null
group by pais
order by qtt_missoes_sucesso;

-- Quais foram os paises que realizaram pelo menos uma missao com qualquer tipo de falha?
select pais, count(status_missao) as qtt_missoes_falha
from space_tratado
where status_missao like "Falha%" and pais is not null
group by pais
order by qtt_missoes_falha;

-- Quais foram os paises que todas as missoes realizadas falharam?
select pais, sum(case when status_missao like "Falha%" then 1 else 0 end) as qdd_falhas,
count(id_lancamento) as total 
from space_tratado
where pais is not null
group by pais
having qdd_falhas = total;

-- Quais foram os paises que todas as missoes realizadas tiveram sucesso?
select pais, sum(case when status_missao like "Sucesso" then 1 else 0 end) as qdd_sucesso,
count(id_lancamento) as total 
from space_tratado
where pais is not null
group by pais
having qdd_sucesso = total;

-- Baseado no histórico, quais os top 5 anos que mais missoes foram realizadas?
select data_ano as ano, count(data_ano) as qtt_m_ano 
from space_tratado
group by ano
order by qtt_m_ano desc limit 5;  

-- Baseado no histórico, quais foram os 3 meses que mais missoes foram realizadas?
select data_mes as mes, count(data_mes) as qtt_m_mes 
from space_tratado
group by mes
order by qtt_m_mes desc limit 3;

-- Qual horário teve mais lancamentos? quantos lancamentos? Mostre o ranking dos top 15 horários preferidos.
select data_hora as hora,
count(data_hora) as qdd
from space_tratado
group by data_hora
order by qdd desc limit 15;

-- Qual foi a data do primeiro lançamento, segundo a base? qual foi o pais, modelo do foguete e objetivo?
select junta_data_hora as data_primeiro_lancamento, pais, modelo_foguete, objetivo_missao
from space_tratado
where junta_data_hora = (select min(junta_data_hora) from space_tratado2);

-- Qual foi a data do ultimo lançamento, segundo a base? qual foi o pais, modelo do foguete e objetivo?
select junta_data_hora as data_lancamento_recente, pais, modelo_foguete, objetivo_missao
from space_tratado
where junta_data_hora = (select max(junta_data_hora) from space_tratado2);

-- qual o valor medio do custo entre os lancamentos?
select truncate(avg(custo_projeto),2) as media_custo_projeto
from space_tratado
where custo_projeto is not null;

-- quais paises gastaram ou tiveram custos com lancamento acima da media?
select pais, custo_projeto as custo_acima_media
from space_tratado
where custo_projeto > (select avg(custo_projeto) from space_tratado2)
group by pais;

-- Quantos lancamentos aconteceram a partir do periodo de 01/01/2020 até o dia de hoje?
select 
count(*) as qdd_periodo
from space_tratado
where junta_data_hora between '2020-01-01 00:00:00' and now();

select 
count(*) from space_tratado
where data_completa between '2020-01-01' and substr(now(),1,10);