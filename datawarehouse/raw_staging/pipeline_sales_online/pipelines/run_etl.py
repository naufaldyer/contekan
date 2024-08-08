import pandas as pd
import numpy as np
import re
import warnings

from transform_blibli import *
from transform_lazada import *
from transform_shopee import *
from transform_shopify import *
from transform_tiktok import *
from transform_tokopedia import *
from transform_zalora import *
from transform_sales_detail_online import run_sales_detail_online


def run_online_etl(year, month):
    run_blibli(year, month)
    run_lazada(year, month)
    run_zalora(year, month)
    run_shopee(year, month)
    run_shopify(year, month)
    run_tokopedia(year, month)
    run_tiktok(year, month)
    run_sales_detail_online(year, month)
