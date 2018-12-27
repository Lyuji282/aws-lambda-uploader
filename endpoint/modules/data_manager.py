# -*- coding: utf-8 -*-

from datetime import datetime as dt, datetime
import gzip
from io import BytesIO
import json
import pickle
import random
import string
import time
import base64
import sys
import traceback

import boto3
from boto3 import Session
from boto3.dynamodb.conditions import Key
import requests
import re


from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.engine import url as sa_url
from sqlalchemy import Sequence
from sqlalchemy.orm.session import sessionmaker

def message_to_slack(url, message):
    try:
        headers = {'content-type': 'application/json'}
        data = {"text": message}
        requests.post(url, data=json.dumps(data), headers=headers)
    except Exception as e:
        print(e)
        raise e


def load_json(file_name):
    with open(file_name, "r") as f:
        return json.load(f)
    return None


def present_date():
    return datetime.now().strftime('%Y-%m-%d')


def rand_maker():
    source_str = 'abcdefghijklmnopqrstuvwxyz'
    return "".join([random.choice(source_str) for x in range(10)])


def make_query_str_values(values):
    ret = ""
    for v in values:
        if v is None:
            ret += "NULL,"
        elif type(v) is str:
            ret += "'%s'," % v
        else:
            ret += "%f," % v
    return ret[:-1]


def load_pickle(file_name):
    with open(file_name, "rb") as f:
        return pickle.load(f)
    return None


def save(data, file_name, mode):
    with open(file_name, mode) as f:
        f.write(data)
    print("saved successfully")


def make_rand_str(digit):
    random_str = ''.join([random.choice(string.ascii_letters + string.digits)
                          for _ in range(digit)])
    return random_str


def unix_to_utc_str(unix_time):
    datetime_obj = dt.fromtimestamp(unix_time)
    return dt_utc_to_str_utc(datetime_obj)


def now_str():
    return dt_utc_to_str_utc(dt.now())


def dt_utc_to_str_utc(dt_utc):
    return dt_utc.strftime('%Y-%m-%d %H:%M:%S')


def data_loader(url):
    r = requests.get(url)
    res = r.content.decode('utf-8')
    return json.loads(res)


def download_image(url, timeout=10):
    response = requests.get(url, allow_redirects=False, timeout=timeout)
    if response.status_code != 200:
        e = Exception("HTTP status: " + response.status_code)
        raise e
    content_type = response.headers["content-type"]
    if 'image' not in content_type:
        e = Exception("Content-Type: " + content_type)
        raise e
    return response.content


def save_image(filename, image):
    with open(filename, "wb") as f:
        f.write(image)


def post(url, params):
    r = requests.post(url, params)
    print("url", url, "params", params)
    res = r.content
    print("raw_res:{}".format(res))
    try:
        print("json.loads:{}".format(json.loads(res)))
        return json.loads(res)
    except:
        print("JSONDecodeError happened")
        print(traceback.format_exc())
        print("res is None")
        return None
    else:
        print("else")
        return None


def get_datetime_now():
    return dt.now().strftime('%Y/%m/%d %H:%M:%S')


# ddm = DynamoDBManager('us-west-2', 'http://localhost:8000')
class DynamoDBManager():
    def __init__(self, region_name, endpoint_url=None, env=None):
        if env == 'localhost':
            self.dynamodb = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url)
            self.client = boto3.client('dynamodb', region_name=region_name, endpoint_url=endpoint_url)
        else:
            self.dynamodb = boto3.resource('dynamodb')
            self.client = boto3.client('dynamodb')

    def create_table(self, table_name, key_schema, attribute_definitions, provisioned_throughput):
        table = self.client.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput=provisioned_throughput
        )
        print("Table status:", table.table_status)

    def put(self, table_name, data):
        table = self.dynamodb.Table(table_name)

        # item = {'name': 'testing_row', 'foo': Decimal('30.40')}
        t = {}
        for k, v in data.items():
            if type(v) == float:
                t[k] = str(v)
            else:
                t[k] = v
        table.put_item(Item=t)

    def batch_write(self, table_name, data, overwrite_by_pkeys=None):
        table = self.dynamodb.Table(table_name)
        if overwrite_by_pkeys is None:
            with table.batch_writer() as batch:
                for d in data:
                    try:
                        for k, v in d.items():
                            try:
                                if type(v) == float:
                                    d[k] = str(v)
                                elif v is None:
                                    d[k] = "None"
                                else:
                                    pass
                            except Exception as e:
                                print(e)
                                print("Failed at second for loop")
                        try:
                            batch.put_item(Item=d)
                        except Exception as e:
                            print(e)
                            print("Fail at batchput")
                    except Exception as e:
                        print(e)
                        print("Failed_at first for-loop")
        elif overwrite_by_pkeys is not None:
            with table.batch_writer(overwrite_by_pkeys=overwrite_by_pkeys) as batch:
                for d in data:
                    for k, v in d.items():
                        if type(v) == float:
                            d[k] = str(v)
                        elif v is None:
                            d[k] = "None"
                        else:
                            pass
                    try:
                        batch.put_item(Item=d)
                    except TypeError as e:
                        print(e)

    def get_all(self, table_name, is_item=True):
        table = self.dynamodb.Table(table_name)
        response = table.scan()
        if is_item:
            response = response["Items"]
            return response
        else:
            return response

    def get_coin_prices(self, perm_code, start="2017-08-15"):
        table = self.dynamodb.Table("cmc_prices")
        ExclusiveStartKey = None

        results = []

        while True:
            if ExclusiveStartKey is None:
                response = table.query(KeyConditionExpression=Key('perm_code').eq(perm_code)
                                                              & Key('target_at').gt(start))
                results = results + response["Items"]
            else:
                response = table.query(KeyConditionExpression=Key('perm_code').eq(perm_code)
                                                              & Key('target_at').gt(start),
                                       ExclusiveStartKey=ExclusiveStartKey)
                results = results + response["Items"]

            if ("LastEvaluatedKey" in response) == True:
                ExclusiveStartKey = response["LastEvaluatedKey"]
            else:
                break
        return results

    def get_all_data(self, table_name, hash_key=None, hash_value=None, start="2017-08-15"):
        table = self.dynamodb.Table(table_name)

        if hash_key is None:
            ExclusiveStartKey = None

            results = []

            while True:
                if ExclusiveStartKey is None:
                    response = table.query(KeyConditionExpression=Key(hash_key).eq(hash_value)
                                                                  & Key('target_at').gt(start))
                    results = results + response["Items"]
                else:
                    response = table.query(KeyConditionExpression=Key(hash_key).eq(hash_value)
                                                                  & Key('target_at').gt(start),
                                           ExclusiveStartKey=ExclusiveStartKey)
                    results = results + response["Items"]

                if ("LastEvaluatedKey" in response) == True:
                    ExclusiveStartKey = response["LastEvaluatedKey"]
                else:
                    break
            return results
        else:
            ExclusiveStartKey = None

            results = []

            while True:
                if ExclusiveStartKey is None:
                    response = table.scan()
                    results = results + response["Items"]
                else:
                    response = table.scan(ExclusiveStartKey=ExclusiveStartKey)
                    results = results + response["Items"]

                if ("LastEvaluatedKey" in response) == True:
                    ExclusiveStartKey = response["LastEvaluatedKey"]
                else:
                    break
            return results

    def get_latest_bid_and_ask(self, table_name, hash_key):
        table = self.dynamodb.Table(table_name)
        res = table.query(
            KeyConditionExpression=Key('id').eq(hash_key),
            Limit=1,
            ScanIndexForward=False
        )["Items"][0]
        exchange = re.sub(r"([^_]+)_([^_]+)_([^_]+)", r"\3", hash_key)
        pair = re.sub(r"([^_]+)_([^_]+)_([^_]+)", r"\1\2", hash_key)
        bid_and_ask = {"bid": res["bid"], "ask": res["ask"], "exchange": exchange, "pair": pair}
        return bid_and_ask

    def get_latest_prices_volumns(self, table_name, hash_key):
        table = self.dynamodb.Table(table_name)
        res = table.query(
            KeyConditionExpression=Key('id').eq(hash_key),
            Limit=1,
            ScanIndexForward=False
        )["Items"][0]
        last_and_volume = (res["last"], res["volume"])
        return last_and_volume

    def get_in_period(self, table_name, start="1000", end="3000", hash_key=None):
        table = self.dynamodb.Table(table_name)
        if hash_key is None:
            res = table.scan(
                FilterExpression=Key('target_at').lt(end)
                                 & Key('target_at').gt(start)
            )["Items"]
        else:
            res = table.query(
                KeyConditionExpression=Key('id').eq(hash_key)
                                       & Key('target_at').lt(end)
                                       & Key('target_at').gt(start)
            )["Items"]
        return res

    def get_hash_keys(self, table_name):
        res_all = self.get_all(table_name=table_name)
        hashes = [a["id"] for a in res_all]
        unique_hashes = list(set(hashes))
        return unique_hashes


class S3Manager:
    def __init__(self, bucket_name, location):
        self.conn = boto3.resource('s3')
        self.bucket_name = bucket_name
        self.s3client = Session().client('s3')
        self.location = location

    def save(self, data, file_name, is_json=True):
        if is_json:
            obj = self.conn.Object(self.bucket_name, file_name)
            obj.put(Body=json.dumps(data, ensure_ascii=False))
        else:
            obj = self.conn.Object(self.bucket_name, file_name)
            obj.put(Body=data)

    def download(self, file_key):
        obj = self.conn.Object(self.bucket_name, file_key)
        response = obj.get()["Body"].read()
        buf = BytesIO(response)
        gzip_f = gzip.GzipFile(fileobj=buf)
        body = gzip_f.read().decode('utf-8')
        return body

    def download_json(self, file_key):
        obj = self.conn.Object(self.bucket_name, file_key)
        response = obj.get()["Body"].read().decode('utf-8')
        json_content = json.loads(response)
        return json_content

    def download_csv(self, file_key):
        obj = self.conn.Object(self.bucket_name, file_key)
        csv_content = obj.get()["Body"].read().decode('utf-8')
        return csv_content

    def get_created_at(self, file_key):
        obj = self.conn.Object(self.bucket_name, file_key)
        return obj.last_modified

    def get_all_file_names(self, prefix='', keys=[], marker=''):
        response = self.s3client.list_objects(Bucket=self.bucket_name,
                                              Prefix=prefix, Marker=marker)
        if 'Contents' in response:
            keys.extend([content['Key'] for content in response['Contents']])
            if 'IsTruncated' in response:
                return self.get_all_file_names(prefix=prefix, keys=keys, marker=keys[-1])
        return keys

    def upload(self, remote_file_key, local_file_name):
        self.s3client.upload_file(local_file_name, self.bucket_name, remote_file_key)


class DatabaseManager():
    def __init__(self, db_name, host, driver_name, user_name, pass_word, port):
        self.db_name = db_name
        self.host = host
        self.user_name = user_name
        self.pass_word = pass_word
        self.port = port
        db_connect_url = sa_url.URL(drivername=driver_name, username=self.user_name, password=self.pass_word,
                                    host=self.host, port=self.port, database=self.db_name)
        Session = sessionmaker()
        self.engine = create_engine(db_connect_url)
        Session.configure(bind=self.engine)
        self.session = Session()
        self.meta = MetaData(self.engine, reflect=True)

    def copy(self, table_name, bucket_name, file_name):
        query = """
        COPY %s
        FROM 's3://%s/%s'
        iam_role 'arn:aws:iam::932615187650:role/data-streamer'
        region 'ap-northeast-1'
        IGNOREHEADER AS 1 csv""" % (
            table_name, bucket_name, file_name
        )
        self.session.execute(query)
        self.session.commit()

    def execute(self, sql):
        self.session.execute(sql)
        self.session.commit()

    def fetch(self, sql):
        res = self.session.execute(sql)
        return res.fetchall()

    def insert(self, sql):
        self.session.execute(sql)
        self.session.commit()

    def delete_all(self, sql):
        self.session.execute(sql)

    def execute_with_rollback(self, sql, is_commit):
        try:
            self.session.execute(sql)
            if is_commit:
                self.session.commit()
                print("コミットを実行し、Postgresにデータを反映しました。。")
        except:
            self.session.rollback()
            print("例外が発生したため、ロールバックを実行しました。")
            self.session.close()
            exit(1)


class RedshiftManager():
    def __init__(self, db_name, host, path_to_driver, user_name, pass_word, port):
        print("red_shitf_connection attempt")
        self.db_name = db_name
        self.host = host
        self.path_to_driver = path_to_driver
        self.user_name = user_name
        self.pass_word = pass_word
        self.port = port
        db_connect_url = sa_url.URL(
            drivername='redshift+psycopg2',
            username=self.user_name,
            password=self.pass_word,
            host=self.host,
            port=self.port,
            database=self.db_name
        )
        print("session started")
        print(db_connect_url)
        Session = sessionmaker()
        self.engine = create_engine(db_connect_url)
        Session.configure(bind=self.engine)
        self.session = Session()
        # self.meta = MetaData(self.engine, reflect=True)

    def copy(self, table_name, bucket_name, file_name, insert_column_names, iam_role):
        print("copy started")
        insert_column_names_str = "(%s)" % ", ".join(insert_column_names)
        print(insert_column_names_str)
        query = """
        COPY %s
            %s
        FROM
            's3://%s/%s'
        iam_role
            'arn:aws:iam::932615187650:role/%s'
        region
            'ap-northeast-1'
        IGNOREHEADER AS 1
        csv""" % (
            table_name, insert_column_names_str, bucket_name, file_name, iam_role
        )
        print(query)
        self.session.execute(query)
        print("done query")
        self.session.commit()

    def execute_query(self, sql):
        res = self.session.execute(sql)
        return res.fetchall()

    def get_coin_url(self, limit_num=3):
        """ Extract coin homepage urls and coin name

        :return: coin_name and urls -> list
        """

        sql = """
    select 
      url
    from 
      coin_webpages
    where 
      type_id = 1
    limit 10
    """
        try:
            res = self.session.execute(sql)
            res = res.fetchall()
            print(res)
            urls = [r[0] for r in res]
        except Exception as e:
            print(e)
            raise e
        else:
            return urls
            print("function get_coin_url done successfully")
        finally:
            print("the end of function get_coin_url")

    def insert_domain_url_and_scraped_links(self, domain_url, scraped_links,
                                            coin_domain_urls):
        for i, link in enumerate(scraped_links):
            if link in coin_domain_urls:
                continue

            sql = sqlalchemy.sql.text("""
          insert into 
            webpages_urls(domain_url, url, url_type) 
          values (:domain_url, :url, :url_type)
        """)
            try:
                self.session.execute(sql, {"domain_url": domain_url, "url": link,
                                           "url_type": 0})
                self.session.commit()
            except:
                self.session.rollback()
                print("error", link)
                continue
            print("{} inserted {}".format(str(i), link))

    def insert_url_text(self, url, text):

        sql = sqlalchemy.sql.text("""
      insert into 
        webpages_contents(url, text) 
      values (:url, :text)
    """)
        try:
            self.session.execute(sql, {"url": url, "text": text})
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            print("error", str(e))

    def get_domain_urls(self, domain_url):
        try:
            sql = """
          select 
            url
          from 
            webpages_urls
          where
            domain_url = '%s'
          ;
        """ % domain_url
            res = self.session.execute(sql)
            res = res.fetchall()

        except Exception as e:
            print(e)
            raise e
        else:
            if res is None:
                return list()
            return [r[0] for r in res]
            print("function get_domain_urls done successfully")

    def get_url_text_dict(self, domain_url):
        sql = """
        SELECT
          webpages_contents.url,
          webpages_contents.text
        FROM
          webpages_contents
        JOIN (
          SELECT
            webpages_contents.url,
            max(webpages_contents.inserted_at) as latest
          from
            webpages_urls
          JOIN webpages_contents
            on webpages_contents.url = webpages_urls.url
          WHERE 
            webpages_urls.domain_url = '%s'
          GROUP BY webpages_contents.url, text
        ) as a ON
          a.url = webpages_contents.url and a.latest = 
          webpages_contents.inserted_at
        ;
    """ % domain_url

        res = self.session.execute(sql)
        if res is None:
            return {}
        return {r[0]: r[1] for r in res}


class KinesisManager():
    def __init__(self):
        pass

    def stream_to_kenesis(self, name, data, partition_key=None, kinesis_type="kinesis"):
        try:
            if kinesis_type == "kinesis":
                response = boto3.client(kinesis_type).put_record(
                    StreamName=name,
                    Data=json.dumps(data),
                    PartitionKey=partition_key
                )
                return response
            elif kinesis_type == "firehose":
                response = boto3.client(kinesis_type).put_record(
                    DeliveryStreamName=name,
                    Record={
                        'Data': json.dumps(data)
                    }
                )
                return response
            else:
                pass

        except Exception as e:
            print(e)

    def save_text(self, link, html, partition_key):
        """ Save text to AWS Kinesis

        :param link:
        :param html:
        :param partition_key:
        :return:
        """
        try:
            data = {"url": link, "text": html}
            response = boto3.client('kinesis').put_record(
                StreamName="dev-data-coin-links-and-texts",
                Data=json.dumps(data),
                PartitionKey=partition_key
            )
            print(response)
            return response
        except Exception as e:
            print(e)
            return None
        else:
            print("function save_text done successfully")
            return None

    def save_links(self, domain_url, links, partition_key):
        """ Save links to AWS Kinesis

        :param domain_url:
        :param links:
        :param partition_key:
        :return:
        """
        try:
            for l in links:
                data = {"domain_url": domain_url, "url": l, "url_type": 0}
                response = boto3.client('kinesis').put_record(
                    StreamName="dev-data-coin-links-and-texts",
                    Data=json.dumps(data),
                    PartitionKey=partition_key
                )
                print(response)
        except Exception as e:
            print(e)
            return None
        else:
            print("function save_links done successfully")
            return None

    def load(self, event, kinesis_type="kinesis"):
        try:
            if kinesis_type == "kinesis":
                payloads = [base64.b64decode(record["kinesis"]["data"]) for record in event["Records"]]
                return payloads
            elif kinesis_type == "firehose":
                payloads = [base64.b64decode(record["data"]) for record in event["records"]]
                return payloads
            else:
                pass
        except Exception as e:
            print(e)

    def load_all(self, event):
        from ast import literal_eval
        output = []
        for num in range(len(event['records'])):
            data = event['records'][num]['data']
            data = base64.b64decode(event['records'][num]['data'])
            data = json.loads(data)
            data = literal_eval(data)
            data = base64.b64encode(json.dumps(data).encode('utf-8')).decode('utf-8')
            # print("data",data)

            output_json = {
                'recordId': int(event['records'][num]['recordId']),
                'result': 'Ok',
                'data': data
            }
            print(output_json)
            output.append(output_json)

        return {'records': output}


if __name__ == '__main__':
    secrets_file_name = "lambda_conf/secrets.json"
    params = load_json(secrets_file_name)
    p_redshift = params["redshift"]
    db_name, host, path_to_driver, user_name, pass_word, port = p_redshift["db_name"], \
                                                                p_redshift["host"], \
                                                                p_redshift["path_to_driver"], \
                                                                p_redshift["user_name"], \
                                                                p_redshift["pass_word"], \
                                                                p_redshift["port"]
    rsm = RedshiftManager(db_name, host, path_to_driver, user_name, pass_word, port)
    session = rsm.session
    TABLE_ID = Sequence('id', start=1000)
    meta = MetaData()
    engine = rsm.engine

    meta.create_all(engine)
