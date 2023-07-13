import os
import io
import logging as logger
import requests
import urllib
import zipfile
from configparser import ConfigParser
from ssl import CERT_REQUIRED, PROTOCOL_TLSv1_2, SSLContext
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra_sigv4.auth import SigV4AuthProvider
from datetime import datetime
from bs4 import BeautifulSoup

import boto3


class BaseClass:
    def __init__(self, config_for):
        self.config = get_config()
        self.logger = get_logger()
        self.config_for = config_for
        self.aws_access_key_id = self.config.get("aws", "aws_access_key_id", fallback=None)
        self.aws_secret_access_key = self.config.get("aws", "aws_secret_access_key", fallback=None)
        self.region_name = self.config.get("aws", "region_name", fallback=None)
        self.bucket_name = self.config.get("aws", "bucket_name", fallback=None)
        self.web_root = self.config.get(config_for, "web_root", fallback=None)
        self.url = self.config.get(config_for, "entity_url", fallback=None)
        self.date_format = self.config.get(config_for, "date_format", fallback=None)
        self.bucket_prefix = self.config.get(config_for, "bucket_prefix", fallback=None)
        self.keyspace_key = self.config.get(config_for, "keyspace_key", fallback=None)
        self.keyspace_table = self.config.get("general", "keyspace_table", fallback=None)
        self.keyspace_name = self.config.get("general", "keyspace_name", fallback=None)

    def get_updated_date(self):
        """Get the last updated date from the website"""

        try:
            page_response = requests.get(self.url)
            if page_response.status_code == 200:
                content = page_response.content.decode('utf-8')
                updated_start_str = '<strong>Last Updated:</strong>'
                updated_end_str = '</div>'
                updated_start_index = content.find(updated_start_str)
                if updated_start_index != -1:
                    updated_start_index += len(updated_start_str)
                    updated_end_index = content.find(updated_end_str, updated_start_index)
                    if updated_end_index != -1:
                        updated_date_str = content[updated_start_index:updated_end_index].strip()
                        updated_date = datetime.strptime(updated_date_str, f'{self.date_format}')
                        self.logger.info(f'Last updated date: {updated_date}')
                        return updated_date
                    else:
                        message = 'Unable to find updated date end index'
                        self.logger.warning(message)
                        self.logger.info(message)
                else:
                    message = 'Unable to find updated date start index'
                    self.logger.warning(message)
                    self.logger.info(message)
            else:
                message = f'Response status code: {page_response.status_code}'
                self.logger.warning(message)
                self.logger.info(message)
                return None
        except Exception as err:
            message = f'Error occurred while fetching updated date: {err}'
            self.logger.error(message)
            self.logger.info(message)
            return None

    def get_download_links(self):
        try:
            url_response = requests.get(self.url)
            if url_response.status_code == 200:
                parser_data = BeautifulSoup(url_response.content, "html.parser")
                down_link_list = []
                keyword_to_check = get_config().get(self.config_for, "keyword_to_check", fallback=None)
                for data in parser_data.find_all("a", href=True):
                    if f'{keyword_to_check}' in data["href"]:
                        download_link = self.web_root + data["href"]
                        if download_link:
                            self.logger.info(
                                f"Download link found: {download_link}"
                            )
                            down_link_list.append(download_link)
                        else:
                            self.logger.warning(
                                "Download link not found on the webpage."
                            )
                return down_link_list
            else:
                self.logger.warning(
                    f"Failed to get URL. Status code: {url_response.status_code}"
                )
                return None
        except Exception as error:
            self.logger.error("Error in get_download_links", error)

    def put_data_to_s3(self, links, folder_name=None):
        for link in links:
            self.logger.info(
                f"Download link found: {link}"
            )
            self.uploading_to_s3(link, folder_name)

    def uploading_to_s3(self, data_link, folder_name):
        try:
            open_url_response = urllib.request.urlopen(data_link)
            zip_files = zipfile.ZipFile(io.BytesIO(open_url_response.read()))
            for zip_file in zip_files.namelist():
                with zip_files.open(zip_file) as f:
                    if folder_name:
                        final_key = f"{self.bucket_prefix}{folder_name}/{zip_file}"
                    else:
                        final_key = f"{self.bucket_prefix}{zip_file}"
                    print(final_key)
                    # Upload the CSV file to S3
                    self.get_s3_client().upload_fileobj(
                        Fileobj=f,
                        Bucket=self.bucket_name,
                        Key=final_key,
                    )
                    self.logger.info(f"Upload successful: {zip_file}")
            self.logger.info("File Uploaded Successfully ")

        except Exception as err:
            self.logger.error("Error in uploading_to_aws", err)

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

    def _query_keyspaces(self, conn, query):
        try:
            result = conn.execute(
                SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            )
            self.logger.info(f"query submitted successfully {query}")
            return result
        except Exception as err:
            message = f"Error occurred while connecting/querying to aws keyspace: {err}"
            self.logger.error(message)

    def get_latest_date(self):
        query = f"SELECT last_batch FROM {self.keyspace_name}.{self.keyspace_table} WHERE key='{self.keyspace_key}';"
        with AWSKeyspaceManager() as conn:
            results = self._query_keyspaces(conn, query)
        if results.current_rows[0][0]:
            return results.current_rows[0][0]

    def update_keyspace(self, value):
        query = f"""UPDATE {self.keyspace_name}.{self.keyspace_table} SET last_batch='{value}'
                     WHERE key='{self.keyspace_key}';"""
        with AWSKeyspaceManager() as conn:
            self._query_keyspaces(conn, query)

    def insert_new_key(self, key, value=None):
        if not value:
            value = "2023-01-01 00:00:00"
        query = f"""INSERT INTO {self.keyspace_name}.{self.keyspace_table} (key, last_batch) VALUES ('{key}','{value}');"""
        with AWSKeyspaceManager() as conn:
            self._query_keyspaces(conn, query)


def get_path():
    return os.path.realpath(os.path.join(os.path.dirname(__file__), '../..'))


class AWSKeyspaceManager:
    def __init__(self):
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        path = os.path.join(get_path(), "src", "sf-class2-root.crt")
        ssl_context.load_verify_locations(path)
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
    path = os.path.join(get_path(), "src", "config.ini")
    _config.read(path)
    return _config


def get_logger():
    logger.basicConfig(level=logger.INFO)
    return logger
