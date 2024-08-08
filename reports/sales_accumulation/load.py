import pandas as pd
import numpy as np
import warnings
import json
import datetime
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", None)


class SalesAkumLoader:
    def __init__(self, month, year):
        self.month = month
        self.year = year
        self.url = f"postgresql+psycopg2://postgres:dsteam141@gateway-LB-0daa0ad89236a16a.elb.ap-southeast-3.amazonaws.com:5432/sales_reports"
        self.engine = create_engine(self.url)

    def insert_data_to_database(self, df, table_name):
        rows_count = df.shape[0]
        df.to_sql(table_name, self.url, if_exists="append", index=False)
        print(f"{rows_count} added!")

    # Function to check if data types match
    def check_data_types(self, row_existing, row_new):
        for col in row_new:
            if (
                col in row_existing
                and not pd.isna(row_existing[col])
                and not pd.isna(row_new[col])
            ):
                if type(row_existing[col]) != type(row_new[col]):
                    return False
        return True

    def update_data_to_database(self, table_name: str, dict_new, dict_existing):
        connection = self.engine.connect()

        if table_name == "sales_akum":
            logging_table_name = "logging_sales_akum"
        else:
            logging_table_name = "logging_sales_promo"

        id_list_success_update = []
        id_list_failed_update = []
        # Loop melalui data baru dan bandingkan dengan data yang ada
        for id_, new_row in dict_new.items():
            if id_ in dict_existing:
                existing_row = dict_existing[id_]

                # Cek jika ada perubahan dan tipe data cocok
                changes = {
                    col: new_row[col]
                    for col in new_row
                    if col in existing_row
                    and existing_row[col] != new_row[col]
                    and not pd.isna(new_row[col])
                }

                if len(changes) != 0:
                    update_query = f"""
                        UPDATE {table_name}
                        SET {', '.join([f"{col} = :{col}" for col in changes])}
                        WHERE id = :id
                    """
                    changes["id"] = id_

                    # Prepare logging data
                    logging_data = {
                        "id": id_,
                        "table_name": table_name,
                        "operation": "update",
                        "changes": json.dumps(changes),
                        "timestamp": datetime.datetime.now(),
                    }

                    # Debugging: Print query and parameters
                    try:
                        # Jalankan query update
                        connection.execute(text(update_query), **changes)
                        id_list_success_update.append(id_)

                        # Insert logging data
                        logging_query = f"""
                            INSERT INTO {logging_table_name} (id, table_name, operation, changes, timestamp)
                            VALUES (:id, :table_name, :operation, :changes, :timestamp)
                        """
                        connection.execute(text(logging_query), **logging_data)

                    except Exception as e:
                        print(f"Error executing update query for id {id_}: {e}")
                        id_list_failed_update.append(id_)

        print(f"{len(id_list_success_update)} rows success updated! ")
        print(f"{len(id_list_failed_update)} rows failed added! ")
        connection.close()

    def load_data_sales_akum(self, df_sales_akum):

        query_data = f""" 
        select *
        from sales_akum
        where month = '{self.month}' """

        df_existing = pd.read_sql(query_data, self.url)

        # Ambil id dari data existing
        df_joined = df_sales_akum.merge(
            df_existing[
                [
                    "id",
                    "dataareaid",
                    "axcode",
                    "order_create_date",
                    "umbrellabrand",
                    "subbrand",
                    "category",
                    "subcategory",
                ]
            ],
            "left",
            on=[
                "dataareaid",
                "axcode",
                "order_create_date",
                "umbrellabrand",
                "subbrand",
                "category",
                "subcategory",
            ],
        )

        # Pisahkan data yang sudah ada dan yang belum ada
        df_joined["sqm"] = df_joined["sqm"].fillna(0)
        df_to_check = df_joined[~df_joined["id"].isna()]
        df_to_check["id"] = df_to_check["id"].astype(int)

        df_to_insert = df_joined[df_joined["id"].isna()].drop(columns=["id"])

        # Ubah menjadi dictionary
        dict_existing = {row["id"]: row.to_dict() for _, row in df_existing.iterrows()}
        dict_new = {
            row["id"]: {
                k: (
                    round(v, 2)
                    if isinstance(v, float)
                    and ("cust_paid_peritem" in k or "hpp_total" in k)
                    else v
                )
                for k, v in row.to_dict().items()  # Lakukan looping untuk row seteleh menjadi dictionary
            }
            for _, row in df_to_check.iterrows()
        }

        self.update_data_to_database(
            table_name="sales_akum",
            dict_new=dict_new,
            dict_existing=dict_existing,
        )

        if df_to_insert.shape[0] != 0:
            self.insert_data_to_database(df=df_to_insert, table_name="sales_akum")

    def load_data_sales_promo(self, df_sales_promo):
        query_data = f""" 
        select *
        from sales_promo
        where date_part('month', date(order_create_date)) = '{self.month}' """

        df_existing = pd.read_sql(query_data, self.url)

        # Ambil id dari data existing
        df_joined = df_sales_promo.merge(
            df_existing[
                [
                    "id",
                    "dataareaid",
                    "axcode",
                    "order_create_date",
                    "promocode",
                    "remarks",
                ]
            ],
            "left",
            on=[
                "dataareaid",
                "axcode",
                "order_create_date",
                "promocode",
                "remarks",
            ],
        )

        # Pisahkan data yang sudah ada dan yang belum ada
        df_joined["sqm"] = df_joined["sqm"].fillna(0)
        df_to_check = df_joined[~df_joined["id"].isna()]
        df_to_check["id"] = df_to_check["id"].astype(int)

        df_to_insert = df_joined[df_joined["id"].isna()].drop(columns=["id"])

        # Ubah menjadi dictionary
        dict_existing = {row["id"]: row.to_dict() for _, row in df_existing.iterrows()}
        dict_new = {
            row["id"]: {
                k: (
                    round(v, 2)
                    if isinstance(v, float)
                    and ("cust_paid_peritem" in k or "hpp_total" in k)
                    else v
                )
                for k, v in row.to_dict().items()  # Lakukan looping untuk row seteleh menjadi dictionary
            }
            for _, row in df_to_check.iterrows()
        }

        self.update_data_to_database(
            table_name="sales_promo",
            dict_new=dict_new,
            dict_existing=dict_existing,
        )

        if df_to_insert.shape[0] != 0:
            self.insert_data_to_database(df=df_to_insert, table_name="sales_promo")
