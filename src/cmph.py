import requests
import io
import urllib
import zipfile

from utils.BaseUtils import BaseClass, AWSKeyspaceManager

import requests
from bs4 import BeautifulSoup
from datetime import datetime


class CMPH(BaseClass):
    def __init__(self):
        super().__init__("cmph")

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
                        updated_date = datetime.strptime(updated_date_str, '%d/%m/%Y')
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

    def get_date_from_db(self):
        with AWSKeyspaceManager() as awskeyspace:
            latest_date = self.get_latest_date(awskeyspace, self.keyspace_name, self.keyspace_table, self.keyspace_key)
        return latest_date

    def update_date_in_db(self, value):
        with AWSKeyspaceManager() as awskeyspace:
            self.update_keyspace(awskeyspace, self.keyspace_name, self.keyspace_table, self.keyspace_key, value)

    def put_data_to_s3(self):
        try:
            url_response = requests.get(self.url)
            if url_response.status_code == 200:
                parser_data = BeautifulSoup(url_response.content, "html.parser")
                for data in parser_data.find_all("a", href=True):
                    if "BasicCompanyData-" in data["href"]:
                        download_link = self.web_root + data["href"]
                        if download_link:
                            self.logger.info(
                                f"Download link found: {download_link}"
                            )
                            self.uploading_to_aws(download_link)
                        else:
                            self.logger.warning(
                                "Download link not found on the webpage."
                            )
            else:
                self.logger.warning(
                    f"Failed to get URL. Status code: {url_response.status_code}"
                )
        except Exception as error:
            self.logger.error("Error in getting_the_file", error)

    def uploading_to_aws(self, data_link):
        try:
            open_url_response = urllib.request.urlopen(data_link)
            zip_files = zipfile.ZipFile(io.BytesIO(open_url_response.read()))
            for zip_file in zip_files.namelist():
                with zip_files.open(zip_file) as f:
                    # Upload the CSV file to S3
                    self.get_s3_client().upload_fileobj(
                        Fileobj=f,
                        Bucket=self.bucket_name,
                        Key=f"{self.bucket_prefix}{zip_file}",
                    )
                    self.logger.info(f"Upload successful: {zip_file}")
            self.logger.info("File Uploaded Successfully ")

        except Exception as err:
            self.logger.error("Error in uploading_to_aws", err)

    def process(self):
        print(self.url)
        current_date = self.get_updated_date()
        print(f"current date: {current_date}")
        last_date = self.get_date_from_db()
        print(f"last_date: {last_date}")
        if current_date > last_date:
            print("Yes")
            # self.put_data_to_s3()
            # self.update_date_in_db(current_date)
        else:
            print("No")
