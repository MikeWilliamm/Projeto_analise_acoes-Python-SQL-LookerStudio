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
import time

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
        url = f'https://investidor10.com.br/api/dividendos/chart/{ticker}/3650/ano/'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(url, headers=headers)
        
        # print(response.text)
        dados = json.loads(response.text)
        df_history = pd.DataFrame(dados)
        df_history.insert(0, 'ticker', ticker)
        # print(df_history)
        
        #Verifica se todos os elementos da coluna são iguais a zero.
        if (df_history['price'] == 0).all():
            print(f'{ticker} -> Todos os elementos da coluna price são iguais a zero.')
            raise('Todos os elementos da coluna price são iguais a zero.')
        
        #Remove linhas com o price igual a 0
        df_history = df_history.drop(df_history.loc[df_history['price'] == 0].index)
        
        #replace no dados
        valores_a_substituir = {
        'Atual': str(datetime.datetime.now().year),
        }
        df_history['created_at'] = df_history['created_at'].replace(valores_a_substituir)
        

    except Exception as e:
        print(f'{ticker} -> ERRO Exporta dados ticker: {e}')
        return pd.DataFrame()
    else:
        return df_history
    
def main():
    df_tickers = return_df_from_bigquery('select distinct o.ticker as tickers from `acoes-378306.acoes.data_acoes` as o order by o.ticker')
    # print(df_tickers)
    contador = 0
    # df_tickers = ['TAEE11']
    df_hitory_full = pd.DataFrame()
    for ticker in df_tickers['tickers']:
            time.sleep(5)
        # if ticker == 'ITUB3':
            contador += 1
            print(f'Extraindo dados: {ticker} | {contador} - {len(df_tickers["tickers"])}')
            df_history = get_jsons_return_df(ticker)
            if len(df_history) >= 1 or not df_history.empty:
                campos_history = ['ticker', 'created_at', 'price']
                # Orgeniza a ordem da colunas
                df_history = df_history.reindex(columns=campos_history)
                # Remover as linhas com valores nulos nas colunas especificadas
                df_history = df_history.dropna(subset=['created_at', 'price'], how='all')
                df_hitory_full = pd.concat([df_hitory_full, df_history])
    
    df_hitory_full['ticker'] = df_hitory_full['ticker'].astype(str)      
    df_hitory_full['created_at'] = df_hitory_full['created_at'].astype(int)
    df_hitory_full['price'] = df_hitory_full['price'].astype(float)
    print(df_hitory_full.dtypes)
    print(df_hitory_full)
    
    #UPLOAD AO BIG QUERY
    nome_schema = 'acoes'
    nome_tabela = 'acoes_valor_dy'
    table_schema = [
        {'name': 'ticker', 'type': 'STRING'}, {'name': 'created_at', 'type': 'INTEGER'}, {'name': 'price', 'type': 'FLOAT'},
    ]	
    
    orientacion = 'replace'
    upload_df_bq(df_hitory_full, nome_schema, nome_tabela, table_schema, orientacion)
    print('dados porcent_dy importados')
            
main()