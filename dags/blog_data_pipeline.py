import json

import jwt
import pandas as pd
import pendulum
import requests
from google.cloud import bigquery

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


GHOST_ADMIN_API_KEY = Variable.get("GHOST_ADMIN_API_KEY")
GHOST_API_URL = 
GHOST_ADMIN_URL = f"{GHOST_API_URL}/ghost/api/admin"
BUCKET_NAME = 
PROJECT_ID = 
DATASET_ID = 
LOCATION="asia-northeast3"


@dag(start_date=pendulum.datetime(2025, 2, 10, tz='UTC'), schedule='0 1 * * 1', catchup=False)
def blog_data_pipeline():
    @task
    def fetch():
        def generate_jwt():
            id, secret = GHOST_ADMIN_API_KEY.split(':')
            iat = int(pendulum.now().timestamp())
            header = {'alg': 'HS256', 'typ': 'JWT', 'kid': id}
            payload = {'iat': iat, 'exp': iat + 5 * 60, 'aud': '/admin/'}
            return jwt.encode(payload, bytes.fromhex(secret), algorithm='HS256', headers=header)

        def fetch_members():
            token = generate_jwt()
            url = f"{GHOST_ADMIN_URL}/members"
            headers = {'Authorization': f'Ghost {token}'}
            page = 1
            members = []
            while True:
                r = requests.get(f"{url}/?limit=all&page={page}", headers=headers)
                if r.status_code == 401:
                    headers['Authorization'] = f'Ghost {generate_jwt()}'
                    r = requests.get(f"{url}/?limit=all&page={page}", headers=headers)
                if r.status_code == 200:
                    data = r.json()
                    if data['members']:
                        members.extend(data['members'])
                        page += 1
                    else:
                        return members
                else:
                    r.raise_for_status()

        def fetch_newsletters():
            token = generate_jwt()
            url = f"{GHOST_ADMIN_URL}/posts/?filter=tag:newsletters"
            headers = {'Authorization': f'Ghost {token}'}
            r = requests.get(url, headers=headers)
            data = r.json()
            newsletters = pd.DataFrame(data['posts'])
            newsletters = newsletters[newsletters['status']=='published']
            start_date = pendulum.now("Asia/Seoul").subtract(days=7).date()
            end_date = pendulum.now("Asia/Seoul").date()
            newsletters = newsletters[newsletters['published_at'].apply(
                lambda x: start_date <= pd.to_datetime(x).tz_convert('Asia/Seoul').date() < end_date
            )]
            return newsletters.to_dict(orient='records')

        return {
            'members': fetch_members(), 
            'newsletters': fetch_newsletters()
        }


    @task
    def preprocess(data):
        def filter_members(members, lang):
            return members[members['labels'].apply(
                lambda labels: any(label.get('slug') == lang for label in labels)
            )]
        
        def filter_newsletters(newsletters, lang):
            return newsletters[newsletters['email_segment'].str.contains(lang)].squeeze()

        def preprocess_members(func):
            def wrapper(members):
                members = pd.DataFrame(members)
                start_time = pendulum.now('Asia/Seoul').subtract(days=7).start_of('day').add(hours=10)
                end_time = pendulum.now('Asia/Seoul').start_of('day').add(hours=10)
                return [
                    func(filter_members(members, 'english'), start_time, end_time, 'en'), 
                    func(filter_members(members, 'korean'), start_time, end_time, 'ko')
                ]
            return wrapper
        
        def preprocess_newsletters(func):
            def wrapper(newsletters):
                newsletters = pd.DataFrame(newsletters)
                return [
                    func(filter_newsletters(newsletters, 'english'), 'en'), 
                    func(filter_newsletters(newsletters, 'korean'), 'ko')
                ] 
            return wrapper

        @preprocess_members
        def extract_member_statistics(members, start_time, end_time, lang_code):
            date = pendulum.now('Asia/Seoul').format('YYYY-MM-DD')
            total = len(members)
            active = len(members[
                (pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul') >= start_time)
                & (pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul') < end_time)
            ])
            subscriber = len(members[
                (members['subscribed'] == True)
                & (members['email_suppression'].apply(lambda x: x['suppressed'] == False))
            ])
            engagement_rate = round((active / total), 3)
            subscription_rate = round((subscriber / total), 3)
            return {
                'date': date,
                'total': total,
                'active': active,
                'subscriber': subscriber,
                'engagement_rate' : engagement_rate,
                'subscription_rate': subscription_rate,
                'lang_code': lang_code
            } 

        @preprocess_members
        def extract_subscriber_statistics(members, start_time, end_time, lang_code):
            date = pendulum.now('Asia/Seoul').format('YYYY-MM-DD')
            total = len(members[
                (members['subscribed'] == True)
                & (members['email_suppression'].apply(lambda x: x['suppressed'] == False))
            ])
            new = len(members[
                (pd.to_datetime(members['created_at']).dt.tz_convert('Asia/Seoul') >= start_time)
                & (pd.to_datetime(members['created_at']).dt.tz_convert('Asia/Seoul') < end_time)
                & (members['email_suppression'].apply(lambda x: x['suppressed'] == False))
            ])
            churn = len(members[
                (members['subscribed'] == False)
                & (pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul') >= start_time)
                & (pd.to_datetime(members['last_seen_at']).dt.tz_convert('Asia/Seoul') < end_time)
                & (members['email_suppression'].apply(lambda x: x['suppressed'] == False))
            ])
            net = new - churn
            return {
                'date': date,
                'total': total,
                'new': new,
                'churn': churn,
                'net': net,
                'lang_code': lang_code
            }
        

        @preprocess_newsletters
        def extract_newsletter_statistics(newsletter, lang_code):
            date = pendulum.parse(newsletter['published_at']).in_timezone('Asia/Seoul').format('YYYY-MM-DD')
            sent = newsletter['email']['email_count']
            opened = newsletter['email']['opened_count']
            clicked = newsletter['count']['clicks']
            delivered = newsletter['email']['delivered_count']
            open_rate = round((opened / delivered), 3)
            click_rate = round((clicked / delivered), 3)
            delivery_rate = round((delivered / sent), 3)
            return {
                'date': date,
                'sent': sent,
                'opened': opened,
                'clicked': clicked,
                'delivered': delivered,
                'open_rate': open_rate,
                'click_rate': click_rate,
                'delivery_rate': delivery_rate,
                'title': newsletter['title'],
                'lang_code': lang_code
            }

        return {
            'members': data['members'],
            'newsletters': data['newsletters'],
            'member_stats': extract_member_statistics(data['members']),
            'subscriber_stats': extract_subscriber_statistics(data['members']),
            'newsletter_stats': extract_newsletter_statistics(data['newsletters']),
        }


    @task
    def upload(data):
        def upload_s3(raw_data, file_name):
            hook = S3Hook(aws_conn_id="localstack_s3")
            client = hook.get_conn()
            date = pendulum.now('Asia/Seoul').format('YYYYMMDD')
            json_data = json.dumps(raw_data)
            client.put_object(
                Bucket=f"{BUCKET_NAME}",
                Key=f"{date}/{file_name}",
                Body=json_data,
                ContentType="application/json"
            )

        def upload_bq(stats, table_id):
            hook = BigQueryHook(gcp_conn_id="gcp_bq")
            client = hook.get_client()
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
            schema = client.get_table(table_ref).schema
            target_date = stats[0]['date'].replace('-', '')
            job_config = bigquery.LoadJobConfig(
                autodetect=False,
                schema=schema,
                write_disposition="WRITE_TRUNCATE"
            )
            load_job = client.load_table_from_json(stats, f"{table_ref}${target_date}", job_config=job_config)
            load_job.result()

        upload_s3(data['members'], "members.json")
        upload_s3(data['newsletters'], "newsletters.json")
        upload_bq(data['member_stats'], "blog_member_statistics")
        upload_bq(data['subscriber_stats'], "blog_subscriber_statistics")
        upload_bq(data['newsletter_stats'], "blog_newsletter_statistics")

    upload(preprocess(fetch()))


blog_data_pipeline()
