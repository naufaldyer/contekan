import pandas as pd
import numpy as np
import warnings
import boto3

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)


class S3Extractor:
    def __init__(self, bucket_name, region_name="ap-southeast-3"):
        self.bucket_name = bucket_name
        self.s3 = boto3.resource("s3", region_name=region_name)
        self.uri = f"s3://{bucket_name}/"
        self.bucket = self.s3.Bucket(bucket_name)

    def extractDataOrderTransactionIndie(
        self, prefix, source, year, month, company, channel
    ):
        prefix_objs = self.bucket.objects.filter(Prefix=prefix)
        data = pd.DataFrame()

        for obj in prefix_objs:
            if obj.key.endswith(".parquet"):
                parts = obj.key.split("/")
                if (
                    parts[3] == company
                    and parts[4] == channel
                    and parts[2] == source
                    and parts[5].split("=")[1] == year
                    and parts[6].split("=")[1] == month
                ):
                    temp = pd.read_parquet(f"{self.uri + obj.key}").assign(
                        dataareaid=company
                    )
                    data = pd.concat([data, temp])

        return data

    def extractDataMasterProductD365(self, company):
        self.path = f"RawData/D365/Product/{company}/data.parquet"
        self.master_product = pd.read_parquet(f"{self.uri}{self.path}")
        return self.master_product
