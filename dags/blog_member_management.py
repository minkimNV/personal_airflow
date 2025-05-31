import json

import jwt
import pandas as pd
import pendulum
import requests
from google.cloud import bigquery

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

GHOST_ADMIN_API_KEY = Variable.get("GHOST_ADMIN_API_KEY")
GHOST_API_URL = 
GHOST_ADMIN_URL = f"{GHOST_API_URL}/ghost/api/admin/"
PROJECT_ID = 
DATASET_ID = 
TABLE_ID = 
LOCATION="asia-northeast3"
MEMBER_LIMIT = 

def generate_jwt():
    id, secret = GHOST_ADMIN_API_KEY.split(':')
    iat = int(pendulum.now().timestamp())
    header = {'alg': 'HS256', 'typ': 'JWT', 'kid': id}
    payload = {
        'iat': iat,
        'exp': iat + 5 * 60,
        'aud': '/admin/'
    }
    return jwt.encode(payload, bytes.fromhex(secret), algorithm='HS256', headers=header)


def get_bq_client():
    hook = BigQueryHook(gcp_conn_id="gcp_bq")
    return hook.get_client()


def get_pending_members(count):
    client = get_bq_client()
    query = f"""
    SELECT email, label, note
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE status = 'pending'
    ORDER BY RAND()
    LIMIT {count}
    """
    job = client.query(query)
    return job.to_dataframe()


def get_active_members():
    client = get_bq_client()
    query = f"""
    SELECT email
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE status = 'active'
    """
    job = client.query(query)
    return job.to_dataframe()


def update_bq(emails, status):
    client = get_bq_client()
    query = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    SET status = '{status}', updated_at = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND)
    WHERE email IN ({', '.join([f"'{email}'" for email in emails])})
    """
    job = client.query(query)
    job.result()


def upsert_bq(json_data):
    client = get_bq_client()
    query = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS target
        USING (
            SELECT 
                JSON_VALUE(member, "$.email") AS email,
                JSON_VALUE(member, "$.label") AS label,
                JSON_VALUE(member, "$.note") AS note
            FROM UNNEST(JSON_QUERY_ARRAY(@members)) AS member
        ) AS source
        ON target.email = source.email
        WHEN MATCHED THEN
            UPDATE SET 
                status = 'active',
                updated_at = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND)
        WHEN NOT MATCHED THEN
            INSERT (email, label, note, created_at, updated_at, status)
            VALUES (source.email, source.label, source.note, TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND), TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND), 'active')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("members", "STRING", json_data)
        ]
    )
    job = client.query(query, job_config=job_config)
    job.result()
    

@dag(start_date=pendulum.datetime(2025, 2, 17, tz='UTC'), schedule='0 5 * * 1', catchup=False)
def blog_member_management():
    @task
    def fetch():
        token = generate_jwt()
        url = f"{GHOST_ADMIN_URL}/members"
        headers = {'Authorization': f'Ghost {token}'}
        page = 1
        members = []
        while True:
            r = requests.get(f"{url}?limit=all&page={page}", headers=headers)
            if r.status_code == 401:
                headers['Authorization'] = f'Ghost {generate_jwt()}'
                r = requests.get(f"{url}?limit=all&page={page}", headers=headers)
            if r.status_code == 200:
                data = r.json()
                if data['members']:
                    members.extend(data['members'])
                    page += 1
                else:
                    return members
            else:
                r.raise_for_status()

    @task
    def delete(members):
        members = pd.DataFrame(members)
        total = len(members)
        members = members[members['labels'].apply(
            lambda labels: any(label.get('name') != 'Internal' for label in labels)
        )]
        inactive_members = members[
                (members['subscribed']==False)
                | (members['email_suppression'].apply(lambda x: x['suppressed'] == True))
                | ((members['email_count'] > 4) & (members['email_open_rate'] < 25.0)
            )
        ]
        token = generate_jwt()
        url = f"{GHOST_ADMIN_URL}/members"
        headers = {'Authorization': f'Ghost {token}'}
        deleted = 0
        for member_id in inactive_members['id']:
            r = requests.delete(f"{url}/{member_id}", headers=headers)
            if r.status_code == 401:
                headers['Authorization'] = f'Ghost {generate_jwt()}'
                r = requests.delete(f"{url}/{member_id}", headers=headers)
            if r.status_code == 204:
                deleted += 1
            else:
                continue
        return MEMBER_LIMIT - total + deleted
    
    @task
    def create(count):
        pending_members = get_pending_members(count)
        token = generate_jwt()
        url = f"{GHOST_ADMIN_URL}/members"
        headers = {'Authorization': f'Ghost {token}'}
        failed = []
        for _, row in pending_members.iterrows():
            member = {
                'members': [{
                    'email': row['email'],
                    'labels': [{
                        "name": row['label'],
                        "slug": row['label'].lower(),
                    }],
                    'note': row['note'] if row['note'] else ''
                }]
            }
            r = requests.post(f"{url}", json=member, headers=headers)
            if r.status_code == 401:
                headers['Authorization'] = f'Ghost {generate_jwt()}'
                r = requests.post(f"{url}", json=member, headers=headers)
            if r.status_code != 201:
                failed.append(row['email'])
        if failed:
            update_bq(failed, 'failed')

    @task
    def sync(members):
        members = pd.DataFrame(members)
        members['label'] = members['labels'].apply(
            lambda labels: next(iter(labels), {}).get('name')
        )
        members = members[['email', 'label', 'note', 'created_at']]
        active_members = get_active_members()
        new_members = members[~members['email'].isin(active_members['email'])]
        missing_members = active_members[~active_members['email'].isin(members['email'])]
        if not new_members.empty:
            upsert_bq(json.dumps(new_members.to_dict(orient="records")))
        if not missing_members.empty:
            update_bq(missing_members['email'], 'deleted')

    fetch1 = fetch()
    fetch2 = fetch()
    create(delete(fetch1)) >> fetch2 >> sync(fetch2)


blog_member_management()
