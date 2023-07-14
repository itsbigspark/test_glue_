import os
import boto3
from ssl import CERT_REQUIRED, PROTOCOL_TLSv1_2, SSLContext
from cassandra.cluster import Cluster
from cassandra_sigv4.auth import SigV4AuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from src.utils.Common_helpers import get_path, get_logger, get_config


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


def query_keyspaces(conn, query):
    try:
        result = conn.execute(
            SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        )
        get_logger().info(f"query submitted successfully {query}")
        return result
    except Exception as err:
        message = f"Error occurred while connecting/querying to aws keyspace: {err}"
        get_logger().error(message)
