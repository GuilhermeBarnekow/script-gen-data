import os
import logging
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect, ProgrammingError
import holidays
from datetime import datetime
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

load_dotenv()

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD') 
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')  # Opcional
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')  
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')  #

required_vars = {
    'SNOWFLAKE_USER': SNOWFLAKE_USER,
    'SNOWFLAKE_ACCOUNT': SNOWFLAKE_ACCOUNT,
    'SNOWFLAKE_WAREHOUSE': SNOWFLAKE_WAREHOUSE,
    'SNOWFLAKE_DATABASE': SNOWFLAKE_DATABASE,
    'SNOWFLAKE_SCHEMA': SNOWFLAKE_SCHEMA,
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    logging.error(f"Faltando variáveis de ambiente: {', '.join(missing_vars)}")
    exit(1)

if SNOWFLAKE_PRIVATE_KEY_PATH:
    logging.info(f"SNOWFLAKE_PRIVATE_KEY_PATH: {SNOWFLAKE_PRIVATE_KEY_PATH}")
    if not SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:
        logging.error("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE não está definida, mas a chave privada está configurada.")
        exit(1)
else:
    logging.warning("SNOWFLAKE_PRIVATE_KEY_PATH não está definida. Certifique-se de que está usando autenticação por usuário/senha ou defina a chave privada corretamente.")

dias_semana_pt = {
    0: 'Segunda-feira',
    1: 'Terça-feira',
    2: 'Quarta-feira',
    3: 'Quinta-feira',
    4: 'Sexta-feira',
    5: 'Sábado',
    6: 'Domingo'
}

meses_pt = {
    1: 'Janeiro',
    2: 'Fevereiro',
    3: 'Março',
    4: 'Abril',
    5: 'Maio',
    6: 'Junho',
    7: 'Julho',
    8: 'Agosto',
    9: 'Setembro',
    10: 'Outubro',
    11: 'Novembro',
    12: 'Dezembro'
}

def generate_dates(start_date='2024-01-01', end_date='2030-12-31'):
    logging.info("Iniciando geração de datas.")
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({'DATA': date_range})  
    df['DATA_CHAVE'] = df['DATA'].dt.strftime('%Y%m%d').astype(int)
    df['DIA'] = df['DATA'].dt.day.astype(int)
    df['NOME_DIA_SEMANA'] = df['DATA'].dt.weekday.map(dias_semana_pt).astype(str)  
    df['NUMERO_DIA_SEMANA'] = df['DATA'].dt.isocalendar().day.astype(int)
    df['MES'] = df['DATA'].dt.month.astype(int)
    df['NOME_MES'] = df['MES'].map(meses_pt).astype(str)  
    df['TRIMESTRE'] = df['DATA'].dt.quarter.astype(int)
    df['SEMESTRE'] = df['TRIMESTRE'].apply(lambda x: 1 if x in [1,2] else 2).astype(int)
    df['ANO'] = df['DATA'].dt.year.astype(int)
    df['ANO_MES'] = df['DATA'].dt.strftime('%Y%m').astype(str)
    df['ANO_TRIMESTRE'] = (df['ANO'].astype(str) + 'T' + df['TRIMESTRE'].astype(str)).astype(str)
    df['ANO_SEMESTRE'] = (df['ANO'].astype(str) + 'S' + df['SEMESTRE'].astype(str)).astype(str)
    df['FIM_DE_SEMANA'] = df['DATA'].dt.weekday.isin([5,6]).astype(bool)
    df['FERIADO_NACIONAL'] = False
    df['NOME_FERIADO_NACIONAL'] = None
    df['FERIADO_SC'] = False
    df['NOME_FERIADO_SC'] = None
    df['FERIADO_PR'] = False
    df['NOME_FERIADO_PR'] = None
    df['FERIADO_RS'] = False
    df['NOME_FERIADO_RS'] = None
    
    df['NUMERO_SEMANA_ANO'] = df['DATA'].dt.isocalendar().week.astype(int)
    
    def week_of_month(dt):
        first_day = dt.replace(day=1)
        dom = dt.day
        adjusted_dom = dom + first_day.weekday()
        return int((adjusted_dom - 1) / 7) + 1
    
    df['NUMERO_SEMANA_MES'] = df['DATA'].apply(week_of_month).apply(lambda x: f'W{x}').astype(str)
    
    logging.info("Geração de datas concluída.")
    return df

def fetch_holidays(df):
    logging.info("Iniciando busca de feriados.")
    years = df['ANO'].unique()
    br_holidays = holidays.Brazil(years=years)
    sc_holidays = holidays.Brazil(state='SC', years=years)
    pr_holidays = holidays.Brazil(state='PR', years=years)
    rs_holidays = holidays.Brazil(state='RS', years=years)
    
    df['DATA_ONLY_DATE'] = df['DATA'].dt.date
    
    df['FERIADO_NACIONAL'] = df['DATA_ONLY_DATE'].isin(br_holidays).astype(bool)
    df['NOME_FERIADO_NACIONAL'] = df['DATA_ONLY_DATE'].map(br_holidays.get).astype(str)
    
    df['FERIADO_SC'] = df['DATA_ONLY_DATE'].isin(sc_holidays).astype(bool)
    df['NOME_FERIADO_SC'] = df['DATA_ONLY_DATE'].map(sc_holidays.get).astype(str)
    
    df['FERIADO_PR'] = df['DATA_ONLY_DATE'].isin(pr_holidays).astype(bool)
    df['NOME_FERIADO_PR'] = df['DATA_ONLY_DATE'].map(pr_holidays.get).astype(str)
    
    df['FERIADO_RS'] = df['DATA_ONLY_DATE'].isin(rs_holidays).astype(bool)
    df['NOME_FERIADO_RS'] = df['DATA_ONLY_DATE'].map(rs_holidays.get).astype(str)
    
    df.drop(columns=['DATA_ONLY_DATE'], inplace=True)
    logging.info("Busca de feriados concluída.")
    return df

def load_private_key(private_key_path, passphrase=None):
    try:
        with open(private_key_path, 'rb') as key_file:
            p_key = key_file.read()
        private_key = serialization.load_pem_private_key(
            p_key,
            password=passphrase.encode() if passphrase else None,
            backend=default_backend()
        )
        logging.info("Chave privada carregada com sucesso.")
        return private_key
    except Exception as e:
        logging.error(f"Erro ao carregar a chave privada: {e}")
        return None

def load_to_snowflake(df):
    logging.info("Iniciando conexão com o Snowflake.")
    
    try:
        if SNOWFLAKE_PRIVATE_KEY_PATH:
            logging.info("Autenticação com chave privada.")
            private_key = load_private_key(SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)
            if not private_key:
                logging.error("Não foi possível carregar a chave privada. Abordagem interrompida.")
                return
            conn = connect(
                user=SNOWFLAKE_USER,
                account=SNOWFLAKE_ACCOUNT,
                private_key=private_key,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                role=SNOWFLAKE_ROLE
            )
        else:
            logging.info("Autenticação com usuário e senha.")
            conn = connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                role=SNOWFLAKE_ROLE
            )
        cursor = conn.cursor()
        logging.info("Conexão com Snowflake estabelecida com sucesso.")
    except ProgrammingError as e:
        logging.error(f"Erro ao conectar ao Snowflake: {e}")
        return
    except Exception as e:
        logging.error(f"Erro ao conectar ao Snowflake: {e}")
        return
    
    try:
        logging.info("Truncando a tabela DIM_DATE.")
        cursor.execute("TRUNCATE TABLE DIM_DATE")
        logging.info("Tabela DIM_DATE truncada com sucesso.")
        
        df['DATA'] = df['DATA'].dt.strftime('%Y-%m-%d')
        
        df['DATA_CHAVE'] = df['DATA_CHAVE'].astype(int)
        df['DIA'] = df['DIA'].astype(int)
        df['NUMERO_DIA_SEMANA'] = df['NUMERO_DIA_SEMANA'].astype(int)
        df['MES'] = df['MES'].astype(int)
        df['TRIMESTRE'] = df['TRIMESTRE'].astype(int)
        df['SEMESTRE'] = df['SEMESTRE'].astype(int)
        df['ANO'] = df['ANO'].astype(int)
        df['NUMERO_SEMANA_ANO'] = df['NUMERO_SEMANA_ANO'].astype(int)
        df['NUMERO_SEMANA_MES'] = df['NUMERO_SEMANA_MES'].astype(str)
        
        boolean_columns = ['FERIADO_NACIONAL', 'FERIADO_SC', 'FERIADO_PR', 'FERIADO_RS', 'FIM_DE_SEMANA']
        for col in boolean_columns:
            df[col] = df[col].astype(bool)
        
        text_columns = [
            'NOME_DIA_SEMANA', 'NOME_MES', 'ANO_MES', 'ANO_TRIMESTRE', 'ANO_SEMESTRE',
            'NOME_FERIADO_NACIONAL', 'NOME_FERIADO_SC', 'NOME_FERIADO_PR', 'NOME_FERIADO_RS'
        ]
        for col in text_columns:
            df[col] = df[col].fillna('').astype(str)
        
        colunas_tabela = [
            'DATA_CHAVE', 'DATA', 'DIA', 'NOME_DIA_SEMANA', 'NUMERO_DIA_SEMANA', 'MES', 'NOME_MES', 
            'TRIMESTRE', 'SEMESTRE', 'ANO', 'ANO_MES', 'ANO_TRIMESTRE', 'ANO_SEMESTRE', 
            'FERIADO_NACIONAL', 'NOME_FERIADO_NACIONAL', 'FERIADO_SC', 'NOME_FERIADO_SC', 
            'FERIADO_PR', 'NOME_FERIADO_PR', 'FERIADO_RS', 'NOME_FERIADO_RS', 'FIM_DE_SEMANA',
            'NUMERO_SEMANA_ANO', 'NUMERO_SEMANA_MES'
        ]
        df = df[colunas_tabela]
        
        logging.info("Carregando dados para a tabela DIM_DATE.")
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            'DIM_DATE'
        )
        logging.info(f"write_pandas retornou: success={success}, nchunks={nchunks}, nrows={nrows}")
        
        if not success:
            raise Exception("Falha ao inserir dados no Snowflake.")
        
        logging.info(f"Carregados {nrows} registros na tabela DIM_DATE com sucesso.")
    except Exception as e:
        logging.error(f"Erro durante a carga de dados: {e}")
    finally:
        cursor.close()
        conn.close()
        logging.info("Conexão com Snowflake fechada.")

def main():
    df_dates = generate_dates()
    df_holidays = fetch_holidays(df_dates)
    load_to_snowflake(df_holidays)

if __name__ == "__main__":
    main()
