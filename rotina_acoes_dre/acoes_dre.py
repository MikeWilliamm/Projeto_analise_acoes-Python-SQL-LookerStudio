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
        table_schema=table_schema,
    ) 

def return_df_from_bigquery(sql_query):
    client = bigquery.Client(project='acoes-378306')
    query_job = client.query(sql_query)
    results = query_job.result()
    df = results.to_dataframe()
    return df

def get_jsons_return_df(ticker):
    try:
        url = f'https://statusinvest.com.br/acao/getrevenue?code={ticker}&type=2&viewType=0'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(url, headers=headers)
        # print(response.text)
        dados = json.loads(response.text)
        df_history = pd.DataFrame(dados)
        df_history.insert(0, 'ticker', ticker)
        # print(df_history)
    except:
        print(f'ERRO Exporta dados ticker: {ticker}')
        return pd.DataFrame(), pd.DataFrame()
    else:
        return df_history
    
def main():
    df_tickers = return_df_from_bigquery('select distinct o.ticker as tickers from `acoes-378306.acoes.data_acoes` as o')
    # print(df_tickers)
    contador = 0
    # df_tickers = ['TAEE11']
    df_hitory_full = pd.DataFrame()
    for ticker in df_tickers['tickers']:
        contador += 1
        print(f'Extraindo dados: {ticker} | {contador} - {len(df_tickers["tickers"])}')
        df_history = get_jsons_return_df(ticker)
        if len(df_history) >= 1 or not df_history.empty:
            campos_history = ['ticker', 'year', 'quarter', 'receitaLiquida', 'despesas', 'lucroLiquido', 'margemBruta', 'margemEbitda', 'margemEbit', 'margemLiquida']
            # Orgeniza a ordem da colunas
            df_history = df_history.reindex(columns=campos_history)
            # Remover as linhas com valores nulos nas colunas especificadas
            df_history = df_history.dropna(subset=['ticker', 'year', 'quarter', 'receitaLiquida', 'despesas', 'lucroLiquido', 'margemBruta', 'margemEbitda', 'margemEbit', 'margemLiquida'], how='all')
            df_hitory_full = pd.concat([df_hitory_full, df_history])
            

    print(df_hitory_full.dtypes)
    print(df_hitory_full)
    #UPLOAD AO BIG QUERY
    nome_schema = 'acoes'
    nome_tabela = 'acoes_dre'
    table_schema = [
        {'name': 'ticker', 'type': 'STRING'}, {'name': 'year', 'type': 'INTEGER'}, {'name': 'quarter', 'type': 'INTEGER'}, 
        {'name': 'receitaLiquida', 'type': 'FLOAT'}, {'name': 'despesas', 'type': 'FLOAT'}, {'name': 'lucroLiquido', 'type': 'FLOAT'},
        {'name': 'margemBruta', 'type': 'FLOAT'}, {'name': 'margemEbitda', 'type': 'FLOAT'}, {'name': 'margemEbit', 'type': 'FLOAT'},
        {'name': 'margemLiquida', 'type': 'FLOAT'}
    ]	
    
    orientacion = 'replace'
    upload_df_bq(df_hitory_full, nome_schema, nome_tabela, table_schema, orientacion)
    print('dados DRE importados')
            
main()
