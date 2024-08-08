from data_s3_extractor import S3Extractor
from data_transformer import DataTransform


class ETLProcess:
    def __init__(self, bucket_name, prefix, company, channel, year, month):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.company = company
        self.channel = channel
        self.year = year
        self.month = month

    def run(self):
        # Extract Phase
        extractor = S3Extractor(self.bucket_name)
        df_orders = extractor.extractDataOrderTransactionIndie(
            prefix=self.prefix,
            company=self.company,
            channel=self.channel,
            source="orders",
            year=self.year,
            month=self.month,
        )

        df_transactions = extractor.extractDataOrderTransactionIndie(
            prefix=self.prefix,
            company=self.company,
            channel=self.channel,
            source="transactions",
            year=self.year,
            month=self.month,
        )

        df_master_product = extractor.extractDataMasterProductD365(company=self.company)

        # Transform Phase
        transformer = DataTransform(
            df_order=df_orders,
            df_transaction=df_transactions,
            df_master_product=df_master_product,
        )

        df_sales_detail = transformer.transformSalesDetail()
        return df_sales_detail
