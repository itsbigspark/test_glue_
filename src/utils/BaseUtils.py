import logging as logger
from configparser import ConfigParser
from ssl import CERT_REQUIRED, PROTOCOL_TLSv1_2, SSLContext
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra_sigv4.auth import SigV4AuthProvider

import boto3


class BaseClass:
    def __init__(self, config_for):
        self.config = get_config()
        self.logger = get_logger()
        self.aws_access_key_id = self.config.get("aws", "aws_access_key_id", fallback=None)
        self.aws_secret_access_key = self.config.get("aws", "aws_secret_access_key", fallback=None)
        self.region_name = self.config.get("aws", "region_name", fallback=None)
        self.bucket_name = self.config.get("aws", "bucket_name", fallback=None)
        self.web_root = self.config.get(config_for, "web_root", fallback=None)
        self.url = self.config.get(config_for, "entity_url", fallback=None)
        self.bucket_prefix = self.config.get(config_for, "bucket_prefix", fallback=None)
        self.keyspace_key = self.config.get(config_for, "keyspace_key", fallback=None)
        self.keyspace_table = self.config.get("general", "keyspace_table", fallback=None)
        self.keyspace_name = self.config.get("general", "keyspace_name", fallback=None)

    def get_boto_session(self):
        return boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )

    def get_list_of_s3_objs(self, bucket_name=None, prefix=None):
        s3_client = self.get_s3_client()
        paginator = s3_client.get_paginator("list_objects_v2")
        if not bucket_name:
            bucket_name = self.bucket_name
        if not prefix:
            prefix = self.bucket_prefix
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                print(obj["Key"])

    def get_s3_client(self):
        return self.get_boto_session().client("s3")

    def get_s3_resource(self):
        return self.get_boto_session().resource("s3")

    def query_keyspaces(self, conn, query):
        try:
            result = conn.execute(
                SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            )
            self.logger.info(f"query submitted successfully {query}")
            return result
        except Exception as err:
            message = f"Error occurred while connecting/querying to aws keyspace: {err}"
            self.logger.error(message)

    def get_latest_date(self, conn, keyspace_name, table_name, key):
        query = f"SELECT last_batch FROM {keyspace_name}.{table_name} WHERE key='{key}';"
        results = self.query_keyspaces(conn, query)
        if results.current_rows[0][0]:
            return results.current_rows[0][0]

    def update_keyspace(self, conn, keyspace_name, table_name, key, value):
        query = f"""UPDATE {keyspace_name}.{table_name} SET last_batch='{value}' WHERE key='{key}';"""
        self.query_keyspaces(conn, query)


class AWSKeyspaceManager:
    def __init__(self):
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        ssl_context.load_verify_locations("sf-class2-root.crt")
        ssl_context.verify_mode = CERT_REQUIRED

        # use this if you want to use Boto to set the session parameters.
        auth_provider = SigV4AuthProvider(self._get_aws_client())
        self.cluster = Cluster(
            ["cassandra.eu-west-1.amazonaws.com"],
            ssl_context=ssl_context,
            auth_provider=auth_provider,
            port=9142,
        )

    def _get_aws_client(self):
        self.config = get_config()
        self.logger = get_logger()
        aws_access_key_id = self.config.get("aws", "aws_access_key_id", fallback=None)
        aws_secret_access_key = self.config.get("aws", "aws_secret_access_key", fallback=None)
        region_name = self.config.get("aws", "region_name", fallback=None)
        return boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def __enter__(self):
        self.connection = self.cluster.connect()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.shutdown()


def get_config():
    _config = ConfigParser()
    _config.read('config.ini')
    return _config


def get_logger():
    logger.basicConfig(level=logger.INFO)
    return logger
