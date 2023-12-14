#%%
import json
import pandas as pd
from google.cloud import bigquery
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import requests
from google.oauth2 import service_account
import pandas_gbq
from google.auth import credentials
from google.cloud import bigquery
from google.oauth2 import service_account
import platform
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time
import gspread
from google.oauth2 import service_account
from google.cloud import bigquery
import psycopg2
import pandas as pd
import pandas_gbq
from google.cloud import storage
from google.cloud import storage
import csv
import os
import threading
import time
import datetime
from google.cloud import bigquery
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


if platform.system() == 'Linux':
    credentials = service_account.Credentials.from_service_account_file(os.path.dirname(os.path.realpath(__file__))+f'/acoes-378306-8edc5317b91c.json')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.dirname(os.path.realpath(__file__))+f'/acoes-378306-8edc5317b91c.json'
elif platform.system() == 'Windows':
    credentials = service_account.Credentials.from_service_account_file(os.path.dirname(os.path.realpath(__file__))+f'\\acoes-378306-8edc5317b91c.json')
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.dirname(os.path.realpath(__file__))+f'\\acoes-378306-8edc5317b91c.json'
    
def upload_df_bq(df, nome_schema, nome_tabela, table_schema, orientacion):
    global credentials
    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=f'{nome_schema}.{nome_tabela}',
        project_id='acoes-378306',
        credentials=credentials,
        if_exists=orientacion,  # replace, append
        #table_schema=table_schema,
    ) 

def return_df_from_bigquery(sql_query):
    client = bigquery.Client(project='acoes-378306')
    query_job = client.query(sql_query)
    results = query_job.result()
    df = results.to_dataframe()
    return df

def get_jsons_return_df(ticker):
    try:
        # url = f'https://brapi.dev/api/quote/{ticker}?range=5y&interval=1d&fundamental=true&dividends=false'
        url = f'https://brapi.dev/api/quote/{ticker}?range=3mo&interval=1d&fundamental=false&dividends=false&token=5XZHZLhC2LZRVrVgJdBXRB'
        response = requests.get(url)
        dados = json.loads(response.text)
        # dados_cabecalho = str(dados["results"][0]).replace("'", '"').split('"historicalDataPrice"')[0][:-2]+'}'
        # print(dados_cabecalho)
        dados_history = dados["results"][0]['historicalDataPrice']
        
        filter_dados_history = []
        campos_history = ['date', 'open', 'high', 'low', 'close', 'volume', 'adjustedClose']
        for obj_json in dados_history:
            
            history_filtrado = {chave: valor for chave, valor in obj_json.items() if chave in campos_history}
            if len(history_filtrado) == 7:
                filter_dados_history.append(history_filtrado)
        df_history = pd.DataFrame(filter_dados_history)
        df_history.insert(0, 'ticker', ticker)
        df_history['date'] = pd.to_datetime(df_history['date'], unit='s')

        
        #----------------------------------------------------------------------------------#
        campos_cabecalho = ['symbol', 'shortName', 'longName', 'currency', 'regularMarketPrice',
                'regularMarketDayHigh', 'regularMarketDayLow', 'regularMarketDayRange',
                'regularMarketChange', 'regularMarketChangePercent', 'regularMarketTime',
                'marketCap',
                'regularMarketVolume', 'regularMarketPreviousClose', 'regularMarketOpen',
                'averageDailyVolume10Day', 'averageDailyVolume3Month', 'fiftyTwoWeekLowChange',
                'fiftyTwoWeekLowChangePercent',
                'fiftyTwoWeekRange', 'fiftyTwoWeekHighChange', 'fiftyTwoWeekHighChangePercent',
                'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'twoHundredDayAverage',
                'twoHundredDayAverageChange', 'twoHundredDayAverageChangePercent']
        # json_cabecalho = json.loads(dados_cabecalho)
        filter_dados_cabecalho = {chave: valor for chave, valor in dados["results"][0].items() if chave in campos_cabecalho}
        df_cabecalho = pd.DataFrame([filter_dados_cabecalho])
    except:
        print(f'ERRO Exporta dados ticker: {ticker}')
        return pd.DataFrame(), pd.DataFrame()
    else:
        return df_cabecalho, df_history


def executar_query_bigquery(query):
    # Substitua 'seu-projeto' pelo ID do projeto do Google Cloud
    projeto = 'acoes-378306'

    # Crie uma instância do cliente BigQuery
    cliente = bigquery.Client(project=projeto)

    # Execute a consulta
    consulta_job = cliente.query(query)

    # Aguarde a conclusão da consulta
    consulta_job.result()

def main():
    df_tickers = return_df_from_bigquery('select distinct o.ticker as tickers from `acoes-378306.acoes.data_acoes` as o')
    # print(df_tickers)
    contador = 0
    # data = {
    # 'tickers': ['TASA4', 'TAEE11'],
    # }
    # df_tickers = pd.DataFrame(data)
    
    df_cabecalho_full = pd.DataFrame()
    df_hitory_full = pd.DataFrame()
    for ticker in df_tickers['tickers']:
        # time.sleep(10)
        contador += 1
        print(f'Extraindo dados: {ticker} | {contador} - {len(df_tickers["tickers"])}')
        df_cabecalho, df_history = get_jsons_return_df(ticker)
        if len(df_cabecalho) >= 1 and len(df_history) >= 1:
            df_cabecalho_full = pd.concat([df_cabecalho_full, df_cabecalho])
            df_hitory_full = pd.concat([df_hitory_full, df_history])
            
    campos_cabecalho = ['symbol', 'shortName', 'longName', 'currency', 'regularMarketPrice',
                'regularMarketDayHigh', 'regularMarketDayLow', 'regularMarketDayRange',
                'regularMarketChange', 'regularMarketChangePercent', 'regularMarketTime',
                'marketCap',
                'regularMarketVolume', 'regularMarketPreviousClose', 'regularMarketOpen',
                'averageDailyVolume10Day', 'averageDailyVolume3Month', 'fiftyTwoWeekLowChange',
                'fiftyTwoWeekLowChangePercent',
                'fiftyTwoWeekRange', 'fiftyTwoWeekHighChange', 'fiftyTwoWeekHighChangePercent',
                'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'twoHundredDayAverage',
                'twoHundredDayAverageChange', 'twoHundredDayAverageChangePercent']
    df_cabecalho_full = df_cabecalho_full.reindex(columns=campos_cabecalho)
    
    campos_history = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'adjustedClose']
    df_hitory_full = df_hitory_full.reindex(columns=campos_history)
    # Remover as linhas com valores nulos nas colunas especificadas
    df_hitory_full = df_hitory_full.dropna(subset=['open', 'high', 'low', 'close', 'volume', 'adjustedClose'], how='all')
    
    # print(df_hitory_full.dtypes)
    # print(df_cabecalho_full.dtypes)


    #UPLOAD AO BIG QUERY
    nome_schema = 'acoes'
    nome_tabela = 'acoes_cabecalho'
    table_schema = [
        {'name': 'symbol', 'type': 'STRING'}, {'name': 'shortName', 'type': 'STRING'}, {'name': 'longName', 'type': 'STRING'}, 
        {'name': 'currency', 'type': 'STRING'}, {'name': 'regularMarketPrice', 'type': 'FLOAT'}, {'name': 'regularMarketDayHigh', 'type': 'FLOAT'},
        {'name': 'regularMarketDayLow', 'type': 'FLOAT'}, {'name': 'regularMarketDayRange', 'type': 'STRING'}, {'name': 'regularMarketChange', 'type': 'FLOAT'},
        {'name': 'regularMarketChangePercent', 'type': 'FLOAT'}, {'name': 'regularMarketTime', 'type': 'STRING'}, {'name': 'marketCap', 'type': 'FLOAT'},
        {'name': 'regularMarketVolume', 'type': 'FLOAT'}, {'name': 'regularMarketPreviousClose', 'type': 'FLOAT'}, {'name': 'regularMarketOpen', 'type': 'FLOAT'},
        {'name': 'averageDailyVolume10Day', 'type': 'FLOAT'}, {'name': 'averageDailyVolume3Month', 'type': 'FLOAT'}, {'name': 'fiftyTwoWeekLowChange', 'type': 'FLOAT'},
        {'name': 'fiftyTwoWeekLowChangePercent', 'type': 'FLOAT'}, {'name': 'fiftyTwoWeekRange', 'type': 'STRING'}, {'name': 'fiftyTwoWeekHighChange', 'type': 'FLOAT'},
        {'name': 'fiftyTwoWeekHighChangePercent', 'type': 'FLOAT'}, {'name': 'fiftyTwoWeekLow', 'type': 'FLOAT'}, {'name': 'fiftyTwoWeekHigh', 'type': 'FLOAT'},
        {'name': 'twoHundredDayAverage', 'type': 'FLOAT'}, {'name': 'twoHundredDayAverageChange', 'type': 'FLOAT'}, {'name': 'twoHundredDayAverageChangePercent', 'type': 'FLOAT'},
    ]	
    
    orientacion = 'replace'
    upload_df_bq(df_cabecalho_full, nome_schema, nome_tabela, table_schema, orientacion)
    print('dados cabecalho importados')
    


    nome_schema = 'acoes'
    nome_tabela = 'acoes_history'
    table_schema = [
        {'name': 'ticker', 'type': 'STRING'}, {'name': 'date', 'type': 'DATETIME'}, {'name': 'open', 'type': 'FLOAT'}, 
        {'name': 'high', 'type': 'FLOAT'}, {'name': 'low', 'type': 'FLOAT'}, {'name': 'close', 'type': 'FLOAT'},
        {'name': 'volume', 'type': 'FLOAT'},{'name': 'adjustedClose', 'type': 'FLOAT'}
        ]
    orientacion = 'append'
    #faz upload teste antes de fazer delete
    upload_df_bq(df_hitory_full, nome_schema, nome_tabela, table_schema, orientacion)

    min_date = df_hitory_full['date'].min()
    delete_comand = f"delete from `acoes-378306.acoes.acoes_history` where cast(date as date) >= '{str(min_date)[:10]}'"
    executar_query_bigquery(delete_comand)
    print(delete_comand)
    time.sleep(4)
    #upload real
    upload_df_bq(df_hitory_full, nome_schema,nome_tabela, table_schema, orientacion)
    print('dados history importados')
    
    # Converter a coluna 'date' para tipo datetime
    df_hitory_full['date'] = pd.to_datetime(df_hitory_full['date'])

    # Obter a data máxima de cada ticker
    max_date = df_hitory_full.groupby('ticker')['date'].max().reset_index()

    # Filtrar o DataFrame para manter apenas as linhas com a data máxima de cada ticker
    df_valor_atual = df_hitory_full.merge(max_date, on=['ticker', 'date'])
            
    nome_schema = 'acoes'
    nome_tabela = 'acoes_metrica_atual'
    table_schema = [
        {'name': 'ticker', 'type': 'STRING'}, {'name': 'date', 'type': 'DATETIME'}, {'name': 'open', 'type': 'FLOAT'}, 
        {'name': 'high', 'type': 'FLOAT'}, {'name': 'low', 'type': 'FLOAT'}, {'name': 'close', 'type': 'FLOAT'},
        {'name': 'volume', 'type': 'FLOAT'},{'name': 'adjustedClose', 'type': 'FLOAT'}
        ]
    orientacion = 'replace'
    upload_df_bq(df_valor_atual, nome_schema, nome_tabela, table_schema, orientacion)
    print('dados metrica_atual importados')
main()
#%%
# url = 'https://brapi.dev/api/available'
# response = requests.get(url)
# dados = json.loads(response.text)
# tickers_com_f = dados["stocks"]
# tickers_sem_f = list(map(lambda x: x.replace('f', '').replace('F', ''), tickers_com_f))
# tickers_sem_duplicatas = list(set(tickers_sem_f))
# %%
# tickers_sem_duplicatas = ['TAEE11', 'RBIR11', 'REVE11']
# %%
# df_cabecalho_full = pd.DataFrame()
# df_dados_hitory = pd.DataFrame()
# contador = 0
# for ticker in tickers_sem_duplicatas:
#     contador += 1
#     print(f'Extraindo dados: {ticker} | {contador} - {len(tickers_sem_duplicatas)} ')
#     try:
#         url = f'https://brapi.dev/api/quote/{ticker}?range=5y&interval=1d&fundamental=true&dividends=false'
#         response = requests.get(url)
#         dados = json.loads(response.text)
#         dados_cabecalho = str(dados["results"][0]).replace("'", '"').split('"validRanges"')[0][:-2]+'}'
#         dados_history = dados["results"][0]['historicalDataPrice']
        
#     except:
#         print(f'ERRO COLETAR DADOS DA API - TICKER: {ticker}')
#     else:
#         dados_cabecalho = dados_cabecalho.replace('\\x81', '').replace('\\x8d', '')
#         print(dados_cabecalho)
#         json_cabecalho = json.loads(dados_cabecalho)
#         df_cabecalho = pd.DataFrame([json_cabecalho])
#         campos = ['symbol', 'shortName', 'longName', 'currency', 'regularMarketPrice',
#             'regularMarketDayHigh', 'regularMarketDayLow', 'regularMarketDayRange',
#             'regularMarketChange', 'regularMarketChangePercent', 'regularMarketTime',
#             'marketCap',
#             'regularMarketVolume', 'regularMarketPreviousClose', 'regularMarketOpen',
#             'averageDailyVolume10Day', 'averageDailyVolume3Month', 'fiftyTwoWeekLowChange',
#             'fiftyTwoWeekLowChangePercent',
#             'fiftyTwoWeekRange', 'fiftyTwoWeekHighChange', 'fiftyTwoWeekHighChangePercent',
#             'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'twoHundredDayAverage',
#             'twoHundredDayAverageChange', 'twoHundredDayAverageChangePercent']
#         # Reordenar colunas e remover colunas extras
#         df_cabecalho_filter= df_cabecalho.reindex(columns=campos)

#         # Remover colunas extras (caso existam)
#         colunas_extras = set(df_cabecalho_filter.columns) - set(campos)
#         df_cabecalho_filter = df_cabecalho_filter.drop(columns=colunas_extras)
#         df_cabecalho_full = pd.concat([df_cabecalho_full, df_cabecalho_filter])


#         df_dados_history_ticker = pd.DataFrame(dados_history)
#         df_dados_history_ticker['ticker'] = ticker
#         df_dados_history_ticker['date'] = pd.to_datetime(df_dados_history_ticker['date'], unit='s')
        
#         campos = ['date', 'open', 'high', 'low', 'close', 'volume', 'adjustedClose', 'ticker']
#         # Reordenar colunas e remover colunas extras
#         df_history_filter = df_dados_history_ticker.reindex(columns=campos)

#         # Remover colunas extras (caso existam)
#         colunas_extras = set(df_history_filter.columns) - set(campos)
#         df_history_filter = df_history_filter.drop(columns=colunas_extras)
        
#         df_dados_hitory = pd.concat([df_dados_hitory, df_history_filter])
    
#     #break
# # %%
# print('Dados históricos e de cabecalho extraidos!')
# print('------------------------------------ Inicio importação dados cabeçalho')
# colunas_desejadas_cabecalho = ['symbol', 'shortName', 'longName', 'currency', 'regularMarketPrice',
#                     'regularMarketDayHigh', 'regularMarketDayLow', 'regularMarketDayRange',
#                     'regularMarketChange', 'regularMarketChangePercent', 'regularMarketTime',
#                     'marketCap',
#                     'regularMarketVolume', 'regularMarketPreviousClose', 'regularMarketOpen',
#                     'averageDailyVolume10Day', 'averageDailyVolume3Month', 'fiftyTwoWeekLowChange',
#                     'fiftyTwoWeekLowChangePercent',
#                     'fiftyTwoWeekRange', 'fiftyTwoWeekHighChange', 'fiftyTwoWeekHighChangePercent',
#                     'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'twoHundredDayAverage',
#                     'twoHundredDayAverageChange', 'twoHundredDayAverageChangePercent']

# # Reordenar colunas e remover colunas extras
# df_cabecalho_full = df_cabecalho_full.reindex(columns=colunas_desejadas_cabecalho)

# # Remover colunas extras (caso existam)
# colunas_extras = set(df_cabecalho.columns) - set(colunas_desejadas_cabecalho)
# df_cabecalho_full = df_cabecalho_full.drop(columns=colunas_extras)

# #UPLOAD AO BIG QUERY
# nome_schema = 'acoes'
# nome_tabela = 'acoes_cabecalho'
# table_schema = [
#     {'name': 'symbol', 'type': 'STRING'}, {'name': 'shortName', 'type': 'STRING'}, {'name': 'longName', 'type': 'STRING'},
#     {'name': 'currency', 'type': 'STRING'}, {'name': 'regularMarketPrice', 'type': 'FLOAT'}, {'name': 'regularMarketDayHigh', 'type': 'FLOAT'},
#     {'name': 'regularMarketDayLow', 'type': 'FLOAT'}, {'name': 'regularMarketDayRange', 'type': 'STRING'}, {'name': 'regularMarketChange', 'type': 'FLOAT'},
#     {'name': 'regularMarketChangePercent', 'type': 'FLOAT'}, {'name': 'regularMarketTime', 'type': 'STRING'}, {'name': 'marketCap', 'type': 'FLOAT'},
#     {'name': 'regularMarketVolume', 'type': 'FLOAT'}, {'name': 'regularMarketPreviousClose', 'type': 'FLOAT'}, {'name': 'regularMarketOpen', 'type': 'FLOAT'},
#     {'name': 'averageDailyVolume10Day', 'type': 'FLOAT'}, {'name': 'averageDailyVolume3Month', 'type': 'FLOAT'}, {'name': 'fiftyTwoWeekLowChange', 'type': 'FLOAT'},
#     {'name': 'fiftyTwoWeekLowChangePercent', 'type': 'FLOAT'}, {'name': 'fiftyTwoWeekRange', 'type': 'STRING'}, {'name': 'fiftyTwoWeekHighChange', 'type': 'FLOAT'},
#     {'name': 'fiftyTwoWeekHighChangePercent', 'type': 'FLOAT'}, {'name': 'fiftyTwoWeekLow', 'type': 'FLOAT'}, {'name': 'fiftyTwoWeekHigh', 'type': 'FLOAT'},
#     {'name': 'twoHundredDayAverage', 'type': 'FLOAT'}, {'name': 'twoHundredDayAverageChange', 'type': 'FLOAT'}, {'name': 'twoHundredDayAverageChangePercent', 'type': 'FLOAT'},
# ]																																																																																																		
# orientacion = 'replace'
# upload_df_bq(df_cabecalho_full, nome_schema, nome_tabela, table_schema, orientacion)
# print('dados cabecalho importados')

# #----------------------------------------------------------------------------------------------------------------
# nome_schema = 'acoes'
# nome_tabela = 'acoes_history'
# table_schema = [
#     {'name': 'date', 'type': 'DATETIME'}, {'name': 'open', 'type': 'FLOAT'}, {'name': 'high', 'type': 'FLOAT'},
#     {'name': 'low', 'type': 'FLOAT'}, {'name': 'close', 'type': 'FLOAT'}, {'name': 'volume', 'type': 'FLOAT'},
#     {'name': 'adjustedClose', 'type': 'FLOAT'}, {'name': 'ticker', 'type': 'STRING'}
#     ]
# orientacion = 'replace'
# upload_df_bq(df_dados_hitory, nome_schema, nome_tabela, table_schema, orientacion)
# print('dados history importados')

