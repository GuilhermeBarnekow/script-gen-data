#Essa Dag tem como Finalidade gerar o DataFrame de todos os ESTADOS, com a mesma lógica da ANTERIOR.

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from holidays import Brazil
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

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
    df = pd.date_range(start='2024-01-01', end='2030-12-31', freq='D')
    df = pd.DataFrame({'DATA': df})
    df['DATA_CHAVE'] = df['DATA'].dt.strftime('%Y%m%d').astype(int)
    df['DIA'] = df['DATA'].dt.day.astype(int)
    df['NOME_DIA_SEMANA'] = df['DATA'].dt.weekday.map(dias_semana_pt).astype(str)
    df['NUMERO_DIA_SEMANA'] = df['DATA'].dt.isocalendar().day.astype(int)
    df['MES'] = df['DATA'].dt.month.astype(int)
    df['NOME_MES'] = df['MES'].map(meses_pt).astype(str)
    df['TRIMESTRE'] = df['DATA'].dt.quarter.astype(int)
    df['SEMESTRE'] = df['TRIMESTRE'].apply(lambda x: 1 if x in [1, 2] else 2).astype(int)
    df['ANO'] = df['DATA'].dt.year.astype(int)
    df['ANO_MES'] = df['DATA'].dt.strftime('%Y%m').astype(str)
    df['ANO_TRIMESTRE'] = (df['ANO'].astype(str) + 'T' + df['TRIMESTRE'].astype(str)).astype(str)
    df['ANO_SEMESTRE'] = (df['ANO'].astype(str) + 'S' + df['SEMESTRE'].astype(str)).astype(str)
    df['FIM_DE_SEMANA'] = df['DATA'].dt.weekday.isin([5, 6]).astype(bool)
    df['NUMERO_SEMANA_ANO'] = df['DATA'].dt.isocalendar().week.astype(int)

    def week_of_month(dt):
        first_day = dt.replace(day=1)
        dom = dt.day
        adjusted_dom = dom + first_day.weekday()
        return int((adjusted_dom - 1) / 7) + 1

    df['NUMERO_SEMANA_MES'] = df['DATA'].apply(week_of_month).apply(lambda x: f'W{x}').astype(str)

    return df

@task
def fetch_holidays(df: pd.DataFrame) -> pd.DataFrame:
    years = df['ANO'].unique()
    br_holidays = Brazil(years=years)  

    df['DATA_ONLY_DATE'] = df['DATA'].dt.date

    df['FERIADO_NACIONAL'] = df['DATA_ONLY_DATE'].isin(br_holidays).astype(bool)
    df['NOME_FERIADO_NACIONAL'] = df['DATA_ONLY_DATE'].map(br_holidays.get).fillna('').astype(str)

    br_states = Brazil.subdivisions

    for state_code in br_states:
        state_holidays = Brazil(state=state_code, years=years)

        col_name_bool = f'FERIADO_{state_code}'
        col_name_str = f'NOME_FERIADO_{state_code}'

        df[col_name_bool] = df['DATA_ONLY_DATE'].isin(state_holidays).astype(bool)
        df[col_name_str] = df['DATA_ONLY_DATE'].map(state_holidays.get).fillna('').astype(str)

    df.drop(columns=['DATA_ONLY_DATE'], inplace=True)

    return df

@task
def load_to_snowflake(df: pd.DataFrame):
    snowflake_conn_id = 'snowflake_default1'
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
    except Exception as e:
        logging.error(f"Erro ao conectar ao Snowflake: {e}")
        raise

    try:
        df['DATA'] = df['DATA'].dt.strftime('%Y-%m-%d')

        int_columns = ['DATA_CHAVE', 'DIA', 'NUMERO_DIA_SEMANA', 'MES', 'TRIMESTRE', 'SEMESTRE', 'ANO', 'NUMERO_SEMANA_ANO']
        for col in int_columns:
            df[col] = df[col].astype(int)

        boolean_columns = ['FERIADO_NACIONAL', 'FIM_DE_SEMANA']

        br_states = Brazil.subdivisions

        for state_code in br_states:
            boolean_columns.append(f'FERIADO_{state_code}')

        for col in boolean_columns:
            df[col] = df[col].astype(bool)

        text_columns = [
            'NOME_DIA_SEMANA', 'NOME_MES', 'ANO_MES', 'ANO_TRIMESTRE', 'ANO_SEMESTRE',
            'NOME_FERIADO_NACIONAL', 'NUMERO_SEMANA_MES'
        ]

        for state_code in br_states:
            text_columns.append(f'NOME_FERIADO_{state_code}')

        for col in text_columns:
            df[col] = df[col].fillna('').astype(str)

        colunas_tabela = [
            'DATA_CHAVE', 'DATA', 'DIA', 'NOME_DIA_SEMANA', 'NUMERO_DIA_SEMANA', 'MES', 'NOME_MES',
            'TRIMESTRE', 'SEMESTRE', 'ANO', 'ANO_MES', 'ANO_TRIMESTRE', 'ANO_SEMESTRE',
            'FIM_DE_SEMANA', 'NUMERO_SEMANA_ANO', 'NUMERO_SEMANA_MES',
            'FERIADO_NACIONAL', 'NOME_FERIADO_NACIONAL'
        ]

        for state_code in br_states:
            colunas_tabela.append(f'FERIADO_{state_code}')
            colunas_tabela.append(f'NOME_FERIADO_{state_code}')

        df = df[colunas_tabela]

        fixed_columns = [
            ('DATA_CHAVE', 'INT'),
            ('DATA', 'DATE'),
            ('DIA', 'INT'),
            ('NOME_DIA_SEMANA', 'STRING'),
            ('NUMERO_DIA_SEMANA', 'INT'),
            ('MES', 'INT'),
            ('NOME_MES', 'STRING'),
            ('TRIMESTRE', 'INT'),
            ('SEMESTRE', 'INT'),
            ('ANO', 'INT'),
            ('ANO_MES', 'STRING'),
            ('ANO_TRIMESTRE', 'STRING'),
            ('ANO_SEMESTRE', 'STRING'),
            ('FIM_DE_SEMANA', 'BOOLEAN'),
            ('NUMERO_SEMANA_ANO', 'INT'),
            ('NUMERO_SEMANA_MES', 'STRING'),
            ('FERIADO_NACIONAL', 'BOOLEAN'),
            ('NOME_FERIADO_NACIONAL', 'STRING')
        ]

        for state_code in br_states:
            fixed_columns.append((f'FERIADO_{state_code}', 'BOOLEAN'))
            fixed_columns.append((f'NOME_FERIADO_{state_code}', 'STRING'))

        create_table_sql = "CREATE OR REPLACE TABLE DIM_DATE (\n"

        for col_name, col_type in fixed_columns:
            create_table_sql += f"    {col_name} {col_type},\n"

        create_table_sql = create_table_sql.rstrip(',\n') + "\n)"

        cursor.execute(create_table_sql)

        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            'DIM_DATE'
        )

        if not success:
            raise Exception("Falha ao inserir dados no Snowflake.")

    except Exception as e:
        logging.error(f"Erro durante a carga de dados: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        logging.info("Conexão com Snowflake fechada.")

with DAG(
    dag_id='generate_and_load_dim_date',
    default_args=default_args,
    description='Gera DIM_DATE e carrega no Snowflake',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    dates_df = generate_dates()
    enriched_df = fetch_holidays(dates_df)
    load_to_snowflake(enriched_df)
