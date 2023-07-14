# import io
# import os
# import zipfile
# import requests
# import pandas as pd

from src.utils.BaseUtils import BaseClass


class CMPH(BaseClass):
    def __init__(self):
        super().__init__("cmph")

    # def download_unzip_convert_to_parquet(self, links):
    #     try:
    #         for link in links:
    #             r = requests.get(link, allow_redirects=True)
    #             open('temp.zip', 'wb').write(r.content)
    #             with zipfile.ZipFile("temp.zip") as zObject:
    #                 csv_file_name = zObject.namelist()[0]
    #                 zObject.extractall(os.getcwd())
    #
    #             df = pd.read_csv(csv_file_name)
    #             parquet_file_name = csv_file_name.replace(".csv", ".parquet")
    #             out_buffer = io.BytesIO()
    #             df.to_parquet(out_buffer, index=False)
    #             self.get_s3_client().put_object(
    #                 Body=out_buffer.getvalue(),
    #                 Bucket=self.bucket_name,
    #                 Key=f"{self.bucket_prefix}{parquet_file_name}",
    #             )
    #             os.remove(csv_file_name)
    #             os.remove("temp.zip")
    #     except Exception as err:
    #         self.logger.error("Error in uploading_to_aws", err)

    def process(self):
        current_date = self.get_updated_date()
        print(f"current date: {current_date}")
        last_date = self.get_latest_date()
        print(f"last_date: {last_date}")
        if current_date > last_date:
            folder_name = current_date.strftime("%Y%m%d")
            # links = self.get_download_links()
            # self.put_data_to_s3(links, folder_name)
            self.verify_upload_to_s3(folder_name)
            print("Yes")
        else:
            print("No")
