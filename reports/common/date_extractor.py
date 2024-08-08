import pandas as pd
import datetime


class DateExtractor:
    def __init__(self):
        self.today = datetime.datetime.today() - pd.DateOffset(1, "days")

    def get_date_refresh(self):
        month = self.today.strftime("%m")
        year = self.today.strftime("%Y")

        dict_refresh = {}

        if self.today.day == 1:
            yesterday = self.today - pd.DateOffset(1, "months")
            last_month = yesterday.strftime("%m")
            last_year = yesterday.strftime("%Y")
            dict_refresh["last_month"] = [last_year, last_month]

        dict_refresh["this_month"] = [year, month]

        return dict_refresh
