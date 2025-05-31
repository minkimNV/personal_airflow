import textwrap
from datetime import datetime
import pendulum
import pandas as pd
import numpy as np

import logging
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
from airflow.sensors.external_task_sensor import ExternalTaskSensor

def load_query(file_name):
    with open(f"/opt/airflow/queries/{file_name}", "r") as f:
        return f.read()
    
def convert_datetime(df):
    date_cols = [col for col in df.columns if col.endswith('_date')]
    for col in date_cols:
        df[col] = df[col].apply(lambda x: pd.to_datetime(x, errors='coerce') if pd.notnull(x) else pd.NaT)
    return df

def datetime_to_str(df):
    date_cols = [col for col in df.columns if col.endswith('_date')]
    for col in date_cols:
        df[col] = df[col].astype(str).replace("NaT", None).apply(lambda x: str(x) if pd.notna(x) else None)
    return df

@task
def extract():
    context = get_current_context()
    EXECUTION_DATE = context["execution_date"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d")
    BUCKET_NAME = 'membership-history'
    # S3_PREFIX = 'all-memberships/'
    DATASET_NAME = "all_members"
    s3_key = f"{DATASET_NAME}_{EXECUTION_DATE}.csv"

    s3_hook = S3Hook(aws_conn_id = 'localstack_s3')

    try:
        content = s3_hook.read_key(
            key = s3_key,
            bucket_name = BUCKET_NAME
        )
        df = pd.read_csv(StringIO(content))
        return df.to_dict(orient = 'records')
    except  Exception as e:
        logging.error(f"\nFailed : {e}")
        raise

@task
def time_setting(data: list) -> list:
    df = pd.DataFrame(data)
    date_cols = [col for col in df.columns if col.endswith('_date')]
    for col in date_cols:
        df[col] = df[col].apply(lambda x: pd.to_datetime(x, errors='coerce') if pd.notnull(x) else pd.NaT)
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.tz_convert("Asia/Seoul")
    
    mask1 = df["payment_status"].isin(["paid", "first paid", "scheduled"]) & df["payment_date"].notna()
    mask2 = df["payment_status"] == "signup"
    mask3 = df["payment_status"].isin(["churn"]) & df["churn_date"].notna()

    df.loc[mask1, "week_start_date"] = (
        df.loc[mask1, "payment_date"] 
        - pd.to_timedelta(df.loc[mask1, "payment_date"].dt.weekday, unit="D")
    )
    df.loc[mask2, "week_start_date"] = (
        df.loc[mask2, "signup_date"] 
        - pd.to_timedelta(df.loc[mask2, "signup_date"].dt.weekday, unit="D")
    )
    df.loc[mask3, "week_start_date"] = (
        df.loc[mask3, "churn_date"]
        - pd.to_timedelta(df.loc[mask3, "churn_date"].dt.weekday, unit="D")
    )

    df.loc[mask1, 'year_month_date'] = df.loc[mask1, 'payment_date'].dt.strftime('%Y-%m')
    df.loc[mask2, 'year_month_date'] = df.loc[mask2, 'signup_date'].dt.strftime('%Y-%m')
    df.loc[mask3, 'year_month_date'] = df.loc[mask3, 'churn_date'].dt.strftime('%Y-%m')

    df["week_start_date"] = df["week_start_date"].dt.tz_localize(None).dt.date
    df["week_start_date"] = pd.to_datetime(df["week_start_date"])
    df['year_month_date'] = pd.to_datetime(df['year_month_date'], format='%Y-%m').dt.tz_localize(None)

    df = datetime_to_str(df)
    return df.to_dict(orient = 'records')

@task
def analyse(data: list, date_col: str) -> list:
    df = pd.DataFrame(data)
    df = convert_datetime(df)

    # 1. 주별 신규 회원 수 (중복 제거 후 고유값 계산)
    signup_count = (
        df[[date_col, 'identity']].drop_duplicates(subset=['identity'])
        .groupby(date_col)
        .size()
        .reset_index(name='signup_count')
    )

    # 2. 모든 주차 추가
    statistics_df = df[[date_col]].drop_duplicates().sort_values(by =[date_col])
    statistics_df = statistics_df.merge(signup_count, on = date_col, how = 'left').fillna(0)
    
    # 3. 매주 전체 신규 누적 수 계산
    statistics_df['cumsum_signup_count'] = statistics_df['signup_count'].cumsum()
    
    # 4. 주별 신규 유료 회원 수
    first_paid_count = (
        df[df['payment_status'].isin(['first paid'])]
        .groupby(date_col)
        .size()
        .reset_index(name='first_paid_count')
    )
    
    # 5. 주별 이탈 회원 수
    churn_user = (
        df[df['churn_date'].notna()]
        .groupby(date_col)['identity']
        .nunique()
        .reset_index(name='churn_count')
    )
    
    # 6. 유료 멤버십 유지 회원 수 (status가 'paid' 또는 'first paid'이고, 아직 churn되지 않은 회원)
    active_paid_users = df[(df['payment_status'].isin(['paid', 'first paid'])) & (df['churn_date'].isna())]
    active_paid_users = (
        active_paid_users
        .groupby(date_col)['identity']
        .nunique()
        .reset_index(name='active_paid_users')
    )
    
    # 0. 중간 통합
    statistics_df[date_col] = pd.to_datetime(statistics_df[date_col], errors='coerce')
    first_paid_count[date_col] = pd.to_datetime(first_paid_count[date_col], errors='coerce')
    active_paid_users[date_col] = pd.to_datetime(active_paid_users[date_col], errors='coerce')
    churn_user[date_col] = pd.to_datetime(churn_user[date_col], errors='coerce')
    statistics_df = (
        statistics_df
        .merge(first_paid_count, on=date_col, how='left')
        .merge(active_paid_users, on=date_col, how='left')
        .merge(churn_user, on=date_col, how='left')
        .fillna(0)
    )
    
    # 7. 해당 주까지의 유료 멤버십 회원 수 (누적)
    statistics_df['cumsum_active_paid'] = (
        statistics_df['active_paid_users'].cumsum() - statistics_df['churn_count'].cumsum()
    )
    
    # 8. 유료 멤버십으로 가입한 회원 수
    df['signup_date'] = pd.to_datetime(df['signup_date']).dt.date
    df['payment_date'] = pd.to_datetime(df['payment_date']).dt.date

    paid_signup = (
        df[(df['payment_status'] == 'first paid') & (df['signup_date'] == df['payment_date'])]
        .groupby(date_col)['identity']
        .nunique()
        .reset_index(name='paid_signup')
    )
    statistics_df = statistics_df.merge(paid_signup, on = date_col, how = 'left').fillna(0)
    
    # 9. 무료 멤버십으로 가입한 회원 수
    statistics_df['free_signup'] = statistics_df['signup_count'] - statistics_df['paid_signup']
    statistics_df['cumsum_active_free'] = statistics_df['cumsum_signup_count'] - statistics_df['cumsum_active_paid']

    # 10. 무료 - 유료 전환율
    statistics_df['signup_to_paid'] = round(
        (statistics_df['first_paid_count'] / statistics_df['signup_count'])
        .replace([float('inf'), -float('inf')], 0)
        .fillna(0), 4
    )
    statistics_df['free_to_paid'] = round(
        (statistics_df['cumsum_active_paid'] / statistics_df['cumsum_active_free'])
        .replace([float('inf'), -float('inf')], 0)
        .fillna(0), 4
    )
    
    #. 11. 수익
    reven_df = df[[date_col]].drop_duplicates().sort_values(by =[date_col])
    payment_df = df.groupby(date_col)['amount'].sum().reset_index(name='payment')
    refund_df = df.groupby(date_col)['refund'].sum().reset_index(name='refund')
    reven_df = (
        reven_df
        .merge(payment_df, on=date_col, how='left')
        .merge(refund_df, on=date_col, how='left')
        .fillna(0)  # NaN 값 0으로 채우기
    )
    reven_df['weekly_revenue'] = reven_df['payment'] - reven_df['refund']
    reven_df[date_col] = (pd.to_datetime(reven_df[date_col], errors='coerce'))
    statistics_df = statistics_df.merge(reven_df, on = date_col, how = 'left')
    
    statistics_df = datetime_to_str(statistics_df)
    return statistics_df.to_dict(orient = 'records')

@task
def user_metrics(data: list, query: str, date_col: str) ->list:
    statistics_df = pd.DataFrame(data)
    bq_hook = BigQueryHook(gcp_conn_id='gcp_bq', use_legacy_sql=False)
    PROJECT_ID = bq_hook.project_id
    ga4_df = bq_hook.get_pandas_df(query, dialect="standard")
    statistics_df = pd.merge(statistics_df, ga4_df, on=date_col, how='outer')
    statistics_df = statistics_df.fillna(0)
    # 10. visitor to lead 전환율
    statistics_df['visit_to_lead'] = round(
        (statistics_df['signup_count'] / statistics_df['first_visit'])
        .replace([np.inf, -np.inf], 0)
        .fillna(0), 4
    )
    return statistics_df.to_dict(orient = 'records')

@task
def load(df_records: list, table_id: str):
    df = pd.DataFrame(df_records)
    date_cols = [col for col in df.columns if col.endswith('_date')]
    for col in date_cols:
        df[col] = df[col].apply(lambda x: pd.to_datetime(x, errors='coerce') if pd.notnull(x) else pd.NaT)
    try:
        client = BigQueryHook(gcp_conn_id='gcp_bq').get_client()
        PROJECT_ID = client.project
        DATASET_ID = 'deepsales_analytics'
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"

        logging.info(f"BigQuery 적재 시작: {table_id}")
        job_config = bigquery.LoadJobConfig(
            autodetect = True,
            write_disposition = "WRITE_TRUNCATE"
        )
        logging.info(f"데이터프레임 컬럼 타입:\n{df.dtypes}")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logging.info(f"BigQuery 적재 완료: {len(df)}건")

    except Exception as e:
        logging.error(f"❌ BigQuery 적재 실패: {str(e)}")
        raise

@dag(
    dag_id="analysis_fin",
    start_date = pendulum.datetime(2024, 8, 1, tz='UTC'),
    schedule_interval = "10 15 * * *",
    catchup=False
)

def analysis():
    data = extract()
    data = time_setting(data)
    
    weekly_report = analyse.override(task_id = "weekly_user_analysis")(data, date_col = 'week_start_date')
    weekly_report = user_metrics.override(task_id = "weekly_service_analysis")(weekly_report, filename = "wau.sql", date_col = 'week_start_date')
    # weekly_report = user_metrics.override(task_id = "weekly_service_analysis")(weekly_report, query = wau_query, date_col = 'week_start_date')

    monthly_report = analyse.override(task_id = "monthly_user_analysis")(data, date_col = 'year_month_date')
    monthly_report = user_metrics.override(task_id = "monthly_service_analysis")(monthly_report, filename = "mau.sql", date_col = 'year_month_date')
    # monthly_report = user_metrics.override(task_id = "monthly_service_analysis")(monthly_report, query = mau_query, date_col = 'year_month_date')

    load.override(task_id="load_weekly_report")(weekly_report, table_id = '00000_Weekly_Metrics')
    load.override(task_id="load_monthly_report")(monthly_report, table_id = '00000_Monthly_Metrics')

dag_instance = analysis()
