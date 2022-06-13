from bs4 import BeautifulSoup
import requests
import pandas as pd
import xarray as xr
from toolz import pipe
from multiprocessing.dummy import Pool
import multiprocessing
from functools import reduce
from operator import add
import warnings
from functools import partial
warnings.filterwarnings('ignore')

def catalog_url(year : int) -> str:
    return f"https://tds.hycom.org/thredds/catalog/datasets/GLBv0.08/expt_53.X/data/{int(year)}/catalog.xml"

def download_catalog(catalog_url : str):
    return requests.get(catalog_url).text

def parse_catalog(catalog):
    return BeautifulSoup(catalog, 'xml')

def extract_urls(parsed_catalogs):
    return list(filter(lambda x : x is not None, 
                       map(lambda x : x.get('urlPath'), 
                           parsed_catalogs.find_all('dataset'))))    

def get_url_list(from_date, to_date):
    print('Getting url list from HYCOM catalog')
    from_year = from_date.year
    to_year = to_date.year
    pool = Pool(multiprocessing.cpu_count()-1)

    partial_urls = reduce(add, 
                      pool.map(lambda year : pipe(year, catalog_url, download_catalog, parse_catalog, extract_urls), 
                               range(from_year,  to_year +1)))
    full_urls = pool.map(lambda x : 'https://tds.hycom.org/thredds/dodsC/' + x, partial_urls)
    daterange = pool.map(lambda x : str(10000*x.year+100*x.month+x.day), pd.date_range(from_date, to_date, freq='D'))
    to_be_downloaded = list(reduce(add, (pool.map(lambda data: list(filter(lambda x : data in x, full_urls)), daterange))))
    pool.close()                           
    return to_be_downloaded

def download(data_set_url, lat, lon, requested_cols=None):
    print(f"Downloading {data_set_url}...")
    data = xr.open_dataset(data_set_url, 
                               decode_times=False, 
                                decode_cf=False)
    if requested_cols is not None:
        return data[requested_cols].sel(lat=lat, lon=lon, method="nearest").to_dataframe()
    else:
        return data.sel(lat=lat, lon=lon, method="nearest").to_dataframe()

def download_all(url_list, lat, lon, requested_cols=None):
    download_lat_lon = partial(download, lat=lat, lon=lon, requested_cols=requested_cols)
    pool = Pool(multiprocessing.cpu_count()-1)
    list_data = pool.map(download_lat_lon, url_list)
    pool.close()                           
    return pd.concat(list_data)

def download_data(from_date, to_date, lat, lon, requested_cols=None):
    return download_all(get_url_list(from_date, to_date),lat, lon, requested_cols)