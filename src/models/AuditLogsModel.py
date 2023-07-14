import datetime
import random
import string
from src.utils.Keyspace_helper import AWSKeyspaceManager, query_keyspaces


class AuditLogs:
    def __init__(self):
        self.source_name = None  # company house, psc,fca etc...
        self.feed_name = None  # zip file name
        self.glue_job_name = None  # python script name in glue job (use name such as you can derive job type)
        self.job_type = None  # web scrapping, cleaning, etc (from jpb args)
        self.input_count = 0
        self.output_count = 0  #
        self.input_path = None  # web url
        self.output_path = None  # s3 path
        self.audit_status = None  # Matched or Unmatched
        self.created_date = None

    def create_table(self):
        query = """CREATE TABLE IF NOT EXISTS business_screening.audit_logs(
                   id varchar,
                   source_name text,
                   feed_name text,
                   glue_job_name text,
                   input_count bigint,
                   output_count bigint,
                   input_path text,
                   output_path text,
                   audit_status text,
                   created_date timestamp,
                   PRIMARY KEY (id));"""
        with AWSKeyspaceManager() as aws:
            query_keyspaces(aws, query)

    def show_all(self):
        query = """SELECT * FROM business_screening.audit_logs;"""
        with AWSKeyspaceManager() as aws:
            return query_keyspaces(aws, query)

    def delete_table(self):
        query = "DROP TABLE IF EXISTS business_screening.audit_logs;"
        with AWSKeyspaceManager() as aws:
            return query_keyspaces(aws, query)

    def save(self):
        id = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
        created_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        query = f"""INSERT INTO business_screening.audit_logs(
                    id,
                    source_name,
                    feed_name,
                    glue_job_name,
                    input_count,
                    output_count,
                    audit_status,
                    created_date) VALUES
                    ('{id}',
                    '{self.source_name}',
                    '{self.feed_name}',
                    '{self.glue_job_name}',
                     {self.input_count},
                     {self.output_count},
                    '{self.audit_status}',
                    '{created_date}');"""
        print(query)
        with AWSKeyspaceManager() as aws:
            query_keyspaces(aws, query)
