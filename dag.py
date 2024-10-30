from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import holidays
import logging
from datetime import datetime
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

default_args = {
    'owner': 'seu_usuario',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='generate_and_load_dim_date',
    default_args=default_args,
    description='Gera DIM_DATE e carrega no Snowflake',
    schedule_interval='@daily',  
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

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

    @task
    def generate_dates():

        logging.info("Iniciando geração de datas.")
        start_date = '2024-01-01'
        end_date = '2030-12-31'
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

    @task
    def fetch_holidays(df: pd.DataFrame) -> pd.DataFrame:

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

    @task
    def load_to_snowflake(df: pd.DataFrame):
        
        snowflake_conn_id = 'snowflake_default1'  
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        
        logging.info("Iniciando conexão com o Snowflake.")
        
        try:
            conn = hook.get_conn()
            cursor = conn.cursor()
            logging.info("Conexão com Snowflake estabelecida com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao conectar ao Snowflake: {e}")
            raise
        
        try:
            logging.info("Preparando DataFrame para o Snowflake.")
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
            
            logging.info("Criando ou substituindo a tabela DIM_DATE no Snowflake.")
            create_table_sql = """
            CREATE OR REPLACE TABLE DIM_DATE (
                DATA_CHAVE INT,
                DATA DATE,
                DIA INT,
                NOME_DIA_SEMANA STRING,
                NUMERO_DIA_SEMANA INT,
                MES INT,
                NOME_MES STRING,
                TRIMESTRE INT,
                SEMESTRE INT,
                ANO INT,
                ANO_MES STRING,
                ANO_TRIMESTRE STRING,
                ANO_SEMESTRE STRING,
                FERIADO_NACIONAL BOOLEAN,
                NOME_FERIADO_NACIONAL STRING,
                FERIADO_SC BOOLEAN,
                NOME_FERIADO_SC STRING,
                FERIADO_PR BOOLEAN,
                NOME_FERIADO_PR STRING,
                FERIADO_RS BOOLEAN,
                NOME_FERIADO_RS STRING,
                FIM_DE_SEMANA BOOLEAN,
                NUMERO_SEMANA_ANO INT,
                NUMERO_SEMANA_MES STRING
            )
            """
            cursor.execute(create_table_sql)
            logging.info("Tabela DIM_DATE criada ou substituída com sucesso.")
            
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
            raise
        finally:
            cursor.close()
            conn.close()
            logging.info("Conexão com Snowflake fechada.")

    dates_df = generate_dates()
    enriched_df = fetch_holidays(dates_df)
    load_to_snowflake(enriched_df)
