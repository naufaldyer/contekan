import pandas as pd
import boto3
import pytz
import csv
from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



def current_date():
    current_utc = datetime.utcnow()
    utc_plus_7 = pytz.timezone("Asia/Jakarta")
    current = current_utc.replace(tzinfo=pytz.utc).astimezone(utc_plus_7).date()
    
    return current



def mapping_change_dtypes(marketplace):
    date_column_mapping = {
        'Zalora': ('Created at', {'Order Item Id': str, 'Zalora Id': str, 'Order Number': str, 'Created at': str, 'Seller SKU': str, 'Order Item Id' : str}, 0, ['Order Number', 'Seller SKU', 'Order Item Id'], 'Seller SKU'),
        'Shopee': ('Waktu Pesanan Dibuat', {'Nomor Referensi SKU': str, 'Waktu Pesanan Dibuat': str, "Jumlah": str, "Harga Awal": str,
            "Total Diskon": str,
            "Diskon Dari Shopee": str,
            "Voucher Ditanggung Shopee": str,
            "Voucher Ditanggung Penjual": str,
            "Ongkos Kirim Dibayar oleh Pembeli": str,
            "Harga Setelah Diskon": str,
            "Cashback Koin": str,
            "Paket Diskon (Diskon dari Shopee)": str, 
            "Paket Diskon (Diskon dari Penjual)": str,
            "Estimasi Potongan Biaya Pengiriman": str,
            "Ongkos Kirim Pengembalian Barang": str,
            "Perkiraan Ongkos Kirim": str}, 0, ['No. Pesanan', 'Nomor Referensi SKU'], 'Nomor Referensi SKU'),
        'Tokopedia': ('Tanggal Pembayaran', {"Nomor SKU": str, 'Tanggal Pembayaran': str}, 4, ['Nomor Invoice', 'Nomor SKU'], 'Nomor SKU'),
        'Lazada': ('createTime', {"lazadaId": str, "sellerSku": str, "orderItemId": str, "orderNumber": str, 'createTime': str}, 0, ['orderNumber', 'sellerSku', "orderItemId"], 'sellerSku'),
        'Blibli': ('Tanggal Order', {"No. Order": str, "No. Order Item": str, "No. Awb": str, "Merchant SKU": str, 'Tanggal Order': str}, 0, ['No. Order', 'Merchant SKU'], 'Merchant SKU'),
        'Tiktok': ('Created Time', {}, 0, [], []),
        'Shopify': ('Created at', {"Lineitem sku": str, 'Created at': str}, 0, ['Name', 'Lineitem sku'], 'Lineitem sku')
    }
    
    return date_column_mapping.get(marketplace, ('', {}))



def assign_sheet(marketplace):
    if 'Tokopedia' in marketplace:
        return 'Laporan Penjualan'
    elif 'Shopee' in marketplace:
        return 'orders'
    elif 'Zalora' in marketplace:
        return 'Sheet'
    elif 'Lazada' in marketplace:
        return 'sheet1'



def read_file():
    
    '''
    get file last modified file on s3
    '''
    
    bucket = boto3.resource('s3', region_name='ap-southeast-3').Bucket('report-deliverables')
    prefix_objs = bucket.objects.filter(Prefix="RawData/sales_online/")

    filename = []
    last = []
    brand = []
    marketplace = []
    path = []
    
    for obj in prefix_objs: 
        if '.xlsx' or '.csv' in obj.key:
            path.append(f"s3://{bucket.name}/{obj.key}")
            filename.append(obj.key.split('/')[-1])
            last.append(obj.last_modified)
            brand.append(obj.key.split('/')[2])
            marketplace.append(obj.key.split('/')[3])
                
    data = pd.DataFrame({'file' : filename, 'last_modified' : last, 'brand' : brand, 'marketplace': marketplace, 'path': path}).iloc[:,:].sort_values('last_modified')
    data['last_modified'] = pd.to_datetime(data['last_modified']) + timedelta(hours=7)
    data = data.groupby(['brand', 'marketplace'])[['brand', 'marketplace', 'file', 'path', 'last_modified']].tail(1).reset_index(drop = True).sort_values('brand')

    # data = data[data['marketplace'] != 'Zalora']
    return data



def unique_month_condition(unique_month):
    current_ym = current_date().strftime('%Y-%m')
    previous_ym = (current_date() - pd.DateOffset(months=1)).strftime('%Y-%m')

    filtered_months = []

    if current_date().day < 31:
        if current_ym in unique_month:
            filtered_months.append(current_ym)

        if previous_ym in unique_month:
            filtered_months.append(previous_ym)
    else:
        if current_ym in unique_month:
            filtered_months.append(current_ym)

    return filtered_months



def to_s3():
    s3 = boto3.client("s3")
    stagingpath = "s3://mega-dev-lake/Staging/Sales/sales_online"
    ecomrawpath = "s3://mp-users-report/e-commerce"

    source_df = read_file().query("last_modified.dt.date == @current_date() and (~marketplace.str.endswith('.csv'))")
    
    if not source_df.empty:
        for data in source_df['path']:
            brand = data.split('/')[-1].split('_')[-3]
            marketplace = data.split('/')[-1].split('_')[-2]
            
            datecolumn, dtypechange, skiprow, column_to_drop, column = mapping_change_dtypes(marketplace)
                
            print(brand, marketplace)
            # read data
            if 'csv' in data:
                if 'Blibli' in data:
                    fmtdate = "%d/%m/%Y %H:%M"
                    read_df = pd.read_csv(f"{data}", delimiter=",", dtype=dtypechange)
                    unique_month = (pd.to_datetime(read_df[datecolumn], format=fmtdate, dayfirst=True).dt.strftime('%Y-%m')).unique()
                else:
                    read_df = pd.read_csv(f"{data}", dtype=dtypechange)
                    unique_month = (pd.to_datetime(read_df[datecolumn]).dt.strftime('%Y-%m')).unique()
                    
    
                read_df['Brand'] = brand
                read_df['Marketplace'] = marketplace
                filtered_month = unique_month_condition(unique_month)


                for i in filtered_month:
                    fyear = i.split('-')[0]
                    fmonth = i.split('-')[1]
                    fmonthname = pd.to_datetime(i).month_name()
                    
                    if 'Blibli' in data:
                        df = read_df[(pd.to_datetime(read_df[datecolumn], format= fmtdate, dayfirst=True).dt.strftime('%Y') == fyear) & 
                                    (pd.to_datetime(read_df[datecolumn], format= fmtdate, dayfirst=True).dt.strftime('%m') == fmonth)
                                    ]
                    else:
                        df = read_df[(pd.to_datetime(read_df[datecolumn]).dt.strftime('%Y') == fyear) & 
                                    (pd.to_datetime(read_df[datecolumn]).dt.strftime('%m') == fmonth)
                                    ]
                    
                    df[column] = df[column].str.strip().str.replace("\n", "")
                    df.to_csv(f"{stagingpath}/sales/{brand}/{fyear}/month={fmonth}/{marketplace}/{current_date().strftime('%y%m%d')} {brand} {marketplace}.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
                        
                    
                    custompath = "/".join(stagingpath.split('/')[3:])
                    prefix = f"{custompath}/raw/{brand}/{fyear}/{fmonthname}/{marketplace}_{fmonthname}_{brand}.csv"
                    pathfile = f"{stagingpath}/raw/{brand}/{fyear}/{fmonthname}/{marketplace}_{fmonthname}_{brand}.csv"
                    exportfile = f"{ecomrawpath}/raw/{brand}/{fyear}/{fmonthname}/{marketplace}_{fmonthname}_{brand}.csv"
                        
                    response = s3.list_objects_v2(Bucket="mega-dev-lake", Prefix=prefix)

                    if "Contents" in response:
                        if marketplace == 'Blibli':
                            exist_df = pd.read_csv(f"{pathfile}", delimiter=",", dtype=dtypechange)
                        else:
                            exist_df = pd.read_csv(f"{pathfile}", dtype=dtypechange)
                            
                        exist_df = exist_df[~exist_df[datecolumn].isin(df[datecolumn].unique().tolist())]

                        new_df = pd.concat([exist_df, df], ignore_index=True)
                        new_df[column] = new_df[column].str.strip().str.replace("\n", "")
                        # new_df = new_df.drop_duplicates(subset=column_to_drop, keep='last')
                        
                        # export to raw staging
                        new_df.to_csv(pathfile, index=False)
                        print(brand, marketplace, fmonth, " .csv successful export staging raw", end="\r", flush=True)

                        # export to online path raw
                        new_df.to_csv(exportfile, index=False)
                        print(brand, marketplace, fmonth, " .csv successful export", end="\r", flush=True)
                
    
                    else:
                        # export to raw staging
                        df.to_csv(pathfile, index=False)
                        print(brand, marketplace, fmonth, " .csv successful export staging raw", end="\r", flush=True)

                        # export to online path raw
                        df.to_csv(exportfile, index=False)
                        print(brand, marketplace, fmonth, " .csv successful export", end="\r", flush=True)
                
                        
            elif 'xlsx' in data:
                read_df = pd.read_excel(f"{data}", dtype=dtypechange, skiprows=skiprow)
                    
                read_df['Brand'] = brand
                read_df['Marketplace'] = marketplace
                
                
                if 'Tokopedia' in data:
                    fmtdate = "%d-%m-%Y %H:%M:%S"
                    unique_month = (pd.to_datetime(read_df[f'{datecolumn}'], format=fmtdate, dayfirst=True).dt.strftime('%Y-%m')).unique()
                    row = 4
                elif 'Lazada' in data:
                    fmtdate = "%d %b %Y %H:%M"
                    unique_month = (pd.to_datetime(read_df[f'{datecolumn}'], format=fmtdate, dayfirst=True).dt.strftime('%Y-%m')).unique()
                    row = 0
                else:
                    unique_month = (pd.to_datetime(read_df[f'{datecolumn}']).dt.strftime('%Y-%m')).unique()
                    row = 0


                filtered_month = unique_month_condition(unique_month)

                for i in filtered_month:
                    fyear = i.split('-')[0]
                    fmonth = i.split('-')[1]
                    fmonthname = pd.to_datetime(i).month_name()
                    
                    # export to staging
                    if 'Tokopedia' in data:
                        df = read_df[(pd.to_datetime(read_df[datecolumn], format= fmtdate, dayfirst=True).dt.strftime('%Y') == fyear) & 
                                    (pd.to_datetime(read_df[datecolumn], format= fmtdate, dayfirst=True).dt.strftime('%m') == fmonth)
                                    ]
                    elif 'Lazada' in data:
                        df = read_df[(pd.to_datetime(read_df[datecolumn], format= fmtdate, dayfirst=True).dt.strftime('%Y') == fyear) & 
                                    (pd.to_datetime(read_df[datecolumn], format= fmtdate, dayfirst=True).dt.strftime('%m') == fmonth)
                                    ]
                    else:
                        df = read_df[(pd.to_datetime(read_df[datecolumn]).dt.strftime('%Y') == fyear) & 
                                    (pd.to_datetime(read_df[datecolumn]).dt.strftime('%m') == fmonth)
                                    ]
                    

                    df[column] = df[column].str.strip().str.replace("\n", "")
                    df.to_excel(f"{stagingpath}/sales/{brand}/{fyear}/month={fmonth}/{marketplace}/{current_date().strftime('%y%m%d')} {brand} {marketplace}.xlsx", index=False)
                
                
                    # export to raw data e-commerce
                    custompath = "/".join(stagingpath.split('/')[3:])
                    prefix = f"{custompath}/raw/{brand}/{fyear}/{fmonthname}/{marketplace}_{fmonthname}_{brand}.xlsx"
                    pathfile = f"{stagingpath}/raw/{brand}/{fyear}/{fmonthname}/{marketplace}_{fmonthname}_{brand}.xlsx"
                    exportfile = f"{ecomrawpath}/raw/{brand}/{fyear}/{fmonthname}/{marketplace}_{fmonthname}_{brand}.xlsx"
                        
                    response = s3.list_objects_v2(Bucket="mega-dev-lake", Prefix=prefix)
                    
                    # sheetname
                    sheet = assign_sheet(data)

                    export
                    if "Contents" in response:
                        exist_df = pd.read_excel(pathfile, dtype=dtypechange)
                        exist_df = exist_df[~exist_df[datecolumn].isin(df[datecolumn].unique().tolist())]
                        # filtered_exist_df = exist_df[~exist_df[column_to_drop].isin(df[column_to_drop]).all(axis=1)]

                        new_df = pd.concat([exist_df, df], ignore_index=True)
                        new_df[column] = new_df[column].str.strip().str.replace("\n", "")
                        # new_df = new_df.drop_duplicates(subset=column_to_drop, keep='last')
                            
                        if 'index' in new_df:
                            new_df = new_df.drop(columns='index')

                        # export to raw staging
                        new_df.to_excel(pathfile, index=False)
                        print(brand, marketplace, fmonth, " .xlsx successful export to staging raw", end="\r", flush=True)

                        # export to online path raw    
                        new_df.to_excel(exportfile, index=False, sheet_name=sheet, startrow=row)

                        print(brand, marketplace, fmonth, " .xlsx successful export", end="\r", flush=True)
                
                    else:
                        # export to raw staging
                        df.to_excel(pathfile, index=False)
                        print(brand, marketplace, fmonth, " .xlsx successful export to staging raw", end="\r", flush=True)

                        # export to online path raw    
                        df.to_excel(exportfile, index=False, sheet_name=sheet, startrow=row)

                        print(brand, marketplace, fmonth, " .xlsx successfull export", end="\r", flush=True)
                
                        
                        
    else:
        nodata =  read_file().query("last_modified.dt.date != @current_date()")
        
        if not nodata.empty:
            filtered_df = read_file().query("last_modified.dt.date != @current_date()")
            filtered_df['last_modified'] = filtered_df['last_modified'].dt.tz_localize(None)

            year = current_date().strftime('%Y')
            month = current_date().strftime('%m')

            filtered_df.to_excel(f"{stagingpath}/noupdate/year={year}/month={month}/{current_date().strftime('%y%m%d')} noupdate ecommerce.xlsx") 
# --------------------------------------------------------------------------------DAGS--------------------------------------------------------------------------------#


default_args = {"owner": "nasha", "start_date": datetime(2024, 2, 20)}


with DAG(
    default_args=default_args,
    dag_id="online_rpa_to_s3",
    schedule_interval="0 4,5 * * *",
    catchup=False,
    tags=["sales", "indie"],
) as dag:
    date = PythonOperator(
        task_id="current_date",
        python_callable=current_date
    )

    export = PythonOperator(
        task_id="sales_online_rpa_to_s3",
        python_callable=to_s3
    )
    
    all_trigger = DummyOperator(
        task_id="trigger_all",
    )

    trigger_sales = TriggerDagRunOperator(
        task_id="trigger_sales_online", trigger_dag_id="sales_online"
    )

    date >> export >> [all_trigger, trigger_sales]