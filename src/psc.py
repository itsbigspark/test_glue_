# import io
# import os
# import zipfile
# import pandas as pd
# import logging as logger
# import requests
# from bs4 import BeautifulSoup
# from datetime import datetime


from src.utils.BaseUtils import BaseClass


class PSC(BaseClass):
    def __init__(self):
        super().__init__("psc")

    def process(self):
        current_date = self.get_updated_date()
        print(f"current date: {current_date}")
        last_date = self.get_latest_date()
        print(f"last_date: {last_date}")

        if current_date > last_date:
            print("Yes")
            folder_name = current_date.strftime("%Y%m%d")
            links = self.get_download_links()
            self.put_data_to_s3(links, folder_name)
        else:
            print("No")
