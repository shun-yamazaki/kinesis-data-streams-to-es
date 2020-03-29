from datetime import datetime
import json
import logging
import base64
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Lambda Initialized.')

host = 'search-test-from-kinesis-zzpsdvt7to2bkqho6fjysctwwy.ap-northeast-1.es.amazonaws.com'
region = 'ap-northeast-1'
service = 'es'

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                   region, service, session_token=credentials.token)
es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def lambda_handler(event, context):
    try:
        logger.info('Record count is {0}.'.format(len(event['Records'])))
        timestamp = []

        actions = get_actions(event['Records'], timestamp)
        helpers.bulk(es, actions)

        now = datetime.now()
        log_created_at = datetime.strptime(min(timestamp), "%Y/%m/%d %H:%M:%S")
        duration = now - log_created_at
        logger.info('Duration:{0}'.format(str(duration)))
    except:
        logger.error('Exception is occured.', exc_info=True)


def get_actions(records, timestamp):
    for record in records:
        data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        logger.debug('Decoded data {0}'.format(data))
        timestamp.append(json.loads(data)['timestamp'])
        yield {
            '_index': 'test_index',
            '_type': '_doc',
            '_source': data,
        }
