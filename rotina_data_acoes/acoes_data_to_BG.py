import json
from sqlalchemy import create_engine
from bs4 import BeautifulSoup as bs
import time
import pandas as pd
from glob import glob
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import os
import datetime
import psycopg2
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from datetime import date
from dateutil.relativedelta import relativedelta
import requests
import datetime

file_log = 'log_' + str(datetime.datetime.now()).replace(" ","_").replace(".", "_").replace(":", "_") + '.txt'
# Caminho das credenciais geradas para o BigQuery
key_path = r"C:\Users\Mike\Desktop\Projeto acoes Google\chave_json_bg\acoes-378306-8edc5317b91c.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Mike\Desktop\Projeto acoes Google\chave_json_bg\acoes-378306-8edc5317b91c.json'
name_log = f'log_gds_{datetime.datetime.now()}'[:27].replace(' ', '_').replace(':', '-').strip()

def sendEmail():
    with open(f'C:\\Users\\Mike\\Desktop\\Projeto acoes Google\\py\\logs\\{file_log}', 'r+') as file:
        log = file.read()
    if 'ERRO' in log.upper():
        titulo = 'ERRO na rotina data_acoes'
    else:
        titulo = 'SUCESSO na rotina data_acoes'
    log = log.replace('\n', '<br>')

    me = 'mikeekrll@gmail.com'  # E-mail de envio
    you = ['mike-william98@hotmail.com']  # E-mail de recebimento
    msg = MIMEMultipart('alternative')
    msg['Subject'] = titulo
    msg['From'] = 'mikeekrll@gmail.com'
    # E-mail de recebimento
    msg['To'] = 'mike-william98@hotmail.com'
    text = log
    html = f"""\
        <html>
        <head></head>
        <body>
            <font face="Courier New, Courier, monospace">{log}<br></font>
        </body>
        </html>
        """
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    msg.attach(part1)
    msg.attach(part2)
    mail = smtplib.SMTP('smtp.gmail.com', 587)
    mail.ehlo()
    mail.starttls()
    mail.login('mikeekrll@gmail.com', 'xkdxftjbguqbtfdvcox')

    for email in you:
        mail.sendmail(me, email, msg.as_string())
    mail.quit()


def print_msg(msg):
    print(msg)
    with open(f'C:\\Users\Mike\\Desktop\\\Projeto acoes Google\\\py\\\logs\\{file_log}', 'a+') as file:
        file.write(msg)


def create_delete_table():
    start_time = time.time()
    client = bigquery.Client()
    
    create_table = '''
        CREATE TABLE IF NOT exists `acoes-378306.acoes.data_acoes`(
        companyid INTEGER,
        companyname STRING,
        ticker STRING ,
        price FLOAT64 ,
        p_l FLOAT64 ,
        p_vp FLOAT64 ,
        p_ebit FLOAT64 ,
        p_ativo FLOAT64 ,
        ev_ebit FLOAT64 ,
        margembruta FLOAT64 ,
        margemebit FLOAT64 ,
        margemliquida FLOAT64 ,
        p_sr FLOAT64 ,
        p_capitalgiro FLOAT64 ,
        p_ativocirculante FLOAT64 ,
        giroativos FLOAT64 ,
        roe FLOAT64 ,
        roa FLOAT64 ,
        roic FLOAT64 ,
        dividaliquidapatrimonioliquido FLOAT64 ,
        dividaliquidaebit FLOAT64 ,
        pl_ativo FLOAT64 ,
        passivo_ativo FLOAT64 ,
        liquidezcorrente FLOAT64 ,
        peg_ratio FLOAT64 ,
        receitas_cagr5 FLOAT64 ,
        liquidezmediadiaria FLOAT64 ,
        vpa FLOAT64 ,
        lpa FLOAT64 ,
        valormercado FLOAT64 ,
        segmentid FLOAT64,
        sectorid FLOAT64,
        subsectorid FLOAT64,
        subsectorname STRING, 
        segmentname STRING, 
        sectorname STRING,
        lucros_cagr5 FLOAT64,
        dy FLOAT64);'''


    delete_bigquery = '''delete from acoes-378306.acoes.data_acoes where 1=1'''

    
    job = client.query(create_table)
    job = client.query(delete_bigquery)
    job.result()

    create_table = '''
        CREATE TABLE IF NOT exists `acoes-378306.acoes.data_atualizacao`(
        data_hora_att DATETIME);'''

    delete_bigquery = '''delete from acoes-378306.acoes.data_atualizacao where 1=1'''

    job = client.query(create_table)
    job = client.query(delete_bigquery)
    job.result()
    
    print_msg(
        f'\n---Tempo criacao de tabela e delecao de dados:{round(time.time() - start_time,2)}s')


def extract_data():
    start_time = time.time()
    #url = 'https://statusinvest.com.br/category/advancedsearchresult?search={"Sector":"","SubSector":"","Segment":"","my_range":"-20;100","forecast":{"upsideDownside":{"Item1":null,"Item2":null},"estimatesNumber":{"Item1":null,"Item2":null},"revisedUp":true,"revisedDown":true,"consensus":[]},"dy":{"Item1":null,"Item2":null},"p_L":{"Item1":null,"Item2":null},"peg_Ratio":{"Item1":null,"Item2":null},"p_VP":{"Item1":null,"Item2":null},"p_Ativo":{"Item1":null,"Item2":null},"margemBruta":{"Item1":null,"Item2":null},"margemEbit":{"Item1":null,"Item2":null},"margemLiquida":{"Item1":null,"Item2":null},"p_Ebit":{"Item1":null,"Item2":null},"eV_Ebit":{"Item1":null,"Item2":null},"dividaLiquidaEbit":{"Item1":null,"Item2":null},"dividaliquidaPatrimonioLiquido":{"Item1":null,"Item2":null},"p_SR":{"Item1":null,"Item2":null},"p_CapitalGiro":{"Item1":null,"Item2":null},"p_AtivoCirculante":{"Item1":null,"Item2":null},"roe":{"Item1":null,"Item2":null},"roic":{"Item1":null,"Item2":null},"roa":{"Item1":null,"Item2":null},"liquidezCorrente":{"Item1":null,"Item2":null},"pl_Ativo":{"Item1":null,"Item2":null},"passivo_Ativo":{"Item1":null,"Item2":null},"giroAtivos":{"Item1":null,"Item2":null},"receitas_Cagr5":{"Item1":null,"Item2":null},"lucros_Cagr5":{"Item1":null,"Item2":null},"liquidezMediaDiaria":{"Item1":null,"Item2":null},"vpa":{"Item1":null,"Item2":null},"lpa":{"Item1":null,"Item2":null},"valorMercado":{"Item1":null,"Item2":null}}&CategoryType=1'
    url = 'https://statusinvest.com.br/category/advancedsearchresultpaginated?search=%7B%22Sector%22%3A%22%22%2C%22SubSector%22%3A%22%22%2C%22Segment%22%3A%22%22%2C%22my_range%22%3A%22-20%3B100%22%2C%22forecast%22%3A%7B%22upsidedownside%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22estimatesnumber%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22revisedup%22%3Atrue%2C%22reviseddown%22%3Atrue%2C%22consensus%22%3A%5B%5D%7D%2C%22dy%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_l%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22peg_ratio%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_vp%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margembruta%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22margemliquida%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22ev_ebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidaebit%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22dividaliquidapatrimonioliquido%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_sr%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_capitalgiro%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22p_ativocirculante%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roe%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roic%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22roa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezcorrente%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22pl_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22passivo_ativo%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22giroativos%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22receitas_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lucros_cagr5%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22liquidezmediadiaria%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22vpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22lpa%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%2C%22valormercado%22%3A%7B%22Item1%22%3Anull%2C%22Item2%22%3Anull%7D%7D&orderColumn=&isAsc=&page=0&take=2000&CategoryType=1'
    #User agente encontrado do OBJT que contem os dados
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    #dat = {'q': 'goog'}
    response = requests.get(url, headers=headers)
    dados = json.loads(response.text)


    df = pd.DataFrame(dados['list'])
    print(df)
    df.columns = df.columns.str.lower()
    #print(df)
    print_msg('\n---Web Scraper executado com sucesso!')
    print_msg(f'\n---Tempo de extracao: {round(time.time() - start_time)}s')
    df = df.reindex(columns=['companyid',
                             'companyname',
                             'ticker',
                             'price',
                             'p_l',
                             'p_vp',
                             'p_ebit',
                             'p_ativo',
                             'ev_ebit',
                             'margembruta',
                             'margemebit',
                             'margemliquida',
                             'p_sr',
                             'p_capitalgiro',
                             'p_ativocirculante',
                             'giroativos',
                             'roe',
                             'roa',
                             'roic',
                             'dividaliquidapatrimonioliquidov',
                             'dividaliquidaebit',
                             'pl_ativo',
                             'passivo_ativo',
                             'liquidezcorrente',
                             'peg_ratio',
                             'receitas_cagr5',
                             'liquidezmediadiaria',
                             'vpa',
                             'lpa',
                             'valormercado',
                             'segmentid',
                             'sectorid',
                             'subsectorid',
                             'subsectorname',
                             'segmentname',
                             'sectorname',
                             'lucros_cagr5',
                             'dy'])
    return df


def export_df(df) -> None:
    start_time = time.time()
    #Export data acoes
    df.to_csv(r'C:\Users\Mike\Desktop\Projeto acoes Google\csv_export\data_acoes.csv', sep=',', index=False)

    date_time_now = str(datetime.datetime.now())[:19]
    
    #Export data ultima atualizacao
    with open(r'C:\Users\Mike\Desktop\Projeto acoes Google\csv_export\data_atualizacao.csv', 'w+') as file_att:
        file_att.write('data_hora_att')
        file_att.write(f'\n{date_time_now}')
    print_msg(f'\n---Tempo exportcao CSV: {round(time.time() - start_time)}s')

    
def bigquery_import():
    start_time = time.time()
    pasta_csvs = f'C:\\Users\\Mike\\Desktop\\Projeto acoes Google\\csv_export'
    caminhos = []
    print_msg(f'\n===Importação Iniciada: {datetime.datetime.now()}')
    for arq in os.listdir(pasta_csvs):
        #caminhos.append(f'{pasta_csvs}\\{arq}')

        file_path = f'{pasta_csvs}\\{arq}'
        table = str(arq).replace('.csv', '')
        table_id = f'acoes-378306.acoes.{table}'
        #file_path = f'C:\\Users\\Mike\\Desktop\\Projeto acoes Google\\csv_export\\data_acoes.csv'
        client = bigquery.Client()
        try :
            config_append = bigquery.LoadJobConfig(
                source_format = bigquery.SourceFormat.CSV, skip_leading_rows=1, field_delimiter = ',', autodetect = True, #quote_character = '"',
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
            
            with open(file_path, 'rb') as source_file:
            #Uso do método load_table_from_file() para ingerir o arquivo.
                job = client.load_table_from_file(source_file, table_id, job_config=config_append)
                job.result()
                #print('tentativa 1 ok')
        except:
            config_append = bigquery.LoadJobConfig(
                source_format = bigquery.SourceFormat.CSV, skip_leading_rows=1, field_delimiter = ',',# autodetect = True, #quote_character = '"',
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
            with open(file_path, 'rb') as source_file:
                #Uso do método load_table_from_file() para ingerir o arquivo.
                job = client.load_table_from_file(source_file, table_id, job_config=config_append)
                job.result()
                #print('tentativa 2 ok')
                
        table = client.get_table(table_id)
        print_msg(
            f'\n---Table {table} loaded: {table.num_rows} rows and {len(table.schema)} columns to {table_id}')
        print_msg(
            f'\n===Importação finalizada -> Tempo decorido: {round(time.time() - start_time,2)}s')

def import_df_to_bq(df):
    client = bigquery.Client()
    project_id = 'acoes-378306'
    dataset_id = 'acoes'
    table_name = 'teste'
    
    df['price'] = df['price'].astype(float)
    df['p_l'] = df['p_l'].astype(float)
    df['dy'] = df['dy'].astype(float)
    df['p_vp'] = df['p_vp'].astype(float)
    df['p_ebit'] = df['p_ebit'].astype(float)
    df['p_ativo'] = df['p_ativo'].astype(float)
    df['ev_ebit'] = df['ev_ebit'].astype(float)
    df['margembruta'] = df['margembruta'].astype(float)
    df['margemebit'] = df['margemebit'].astype(float)
    df['margemliquida'] = df['margemliquida'].astype(float)
    df['p_sr'] = df['p_sr'].astype(float)
    df['p_capitalgiro'] = df['p_capitalgiro'].astype(float)

    
    # Crie uma referência para a tabela do BigQuery
    table_ref = client.dataset(dataset_id, project=project_id).table(table_name)

    # Crie a tabela no BigQuery (se ela ainda não existir)
    schema = [bigquery.SchemaField(name, 'STRING') for name in df.columns]
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)

    # Insira os dados do DataFrame na tabela do BigQuery
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Aguarda a conclusão do carregamento

    print(f'Dados importados para o BigQuery: {project_id}.{dataset_id}.{table_name}')

def main():
    try:
        print_msg(f'---Data Hora inicio: {datetime.datetime.now()}')
        start_time = time.time()
        print_msg('-'*30)
        df = extract_data()
        if len(df) > 10:
            create_delete_table()
        else:
            raise Exception('\nDados nao extraidos corretamente!')
        export_df(df)
        bigquery_import()
        #import_df_to_bq(df)

    except Exception as erro:
        print_msg(F'\nERRO >> \n {erro}')
        print_msg('-'*30)
    finally:
        print_msg('-'*30)
        print_msg(f'\n---Tempo do script -> {round(time.time() - start_time, 2)}')
        print_msg(f'\n---Data Hora fim: {datetime.datetime.now()}')
        sendEmail()
        print_msg('\nEmail send >>>')


()
        
main()



