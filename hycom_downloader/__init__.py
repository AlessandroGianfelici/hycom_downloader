# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 20:32:00 2022
@author: alessandro gianfelici
"""

from datetime import datetime
import multiprocessing
import warnings
from functools import partial, reduce
from multiprocessing.dummy import Pool
from operator import add
import logging
import pandas as pd
import requests
import xarray as xr
from bs4 import BeautifulSoup
from toolz import pipe

warnings.filterwarnings('ignore')


def catalog_url(year: int) -> str:
    """
    Get the catalog url for a given year

    Args:
        year (int): required calendar year

    Returns:
        str: url
    """
    return f"https://tds.hycom.org/thredds/catalog/datasets/GLBv0.08/expt_53.X/data/{int(year)}/catalog.xml"


def download_catalog(catalog_url: str) -> str:
    """
    Download the content of the catalog web page.

    Args:
        catalog_url (str): url of the catalog

    Returns:
        str: the catalog as plain text
    """
    return requests.get(catalog_url).text


def parse_catalog(catalog: str):
    """
    Parse the catalog using BeautifulSoup's xlm parser.

    Args:
        catalog (str): the catalog content

    """
    return BeautifulSoup(catalog, 'xml')


def extract_urls(parsed_catalogs) -> list:
    """
    Get the list of the url of the ncdf file from the parsed catalog.

    Args:
        parsed_catalogs (_type_): the parsed catalog

    Returns:
        list: list of all the url of the netcdf file
    """
    return list(filter(lambda x: x is not None,
                       map(lambda x: x.get('urlPath'),
                           parsed_catalogs.find_all('dataset'))))


def get_url_list(from_date: datetime, to_date: datetime) -> list:
    """
    Get the list of the netcdf url between the two input dates.

    Args:
        from_date (datetime): lower limit of the desired time range
        to_date (datetime): upper limit of the desired time range

    Returns:
        url_list (list): list of the needed url
    """
    print('Getting url list from HYCOM catalog')
    from_year = from_date.year
    to_year = to_date.year
    pool = Pool(multiprocessing.cpu_count()-1)

    partial_urls = reduce(add,
                          pool.map(lambda year: pipe(year, catalog_url, download_catalog, parse_catalog, extract_urls),
                                   range(from_year,  to_year + 1)))
    full_urls = pool.map(
        lambda x: 'https://tds.hycom.org/thredds/dodsC/' + x, partial_urls)
    daterange = pool.map(lambda x: str(
        10000*x.year+100*x.month+x.day), pd.date_range(from_date, to_date, freq='D'))
    to_be_downloaded = list(reduce(add, (pool.map(lambda data: list(
        filter(lambda x: data in x, full_urls)), daterange))))
    pool.close()
    return to_be_downloaded


def download(data_set_url: str, lat: float, lon: float, requested_cols=None) -> pd.DataFrame:
    """
    Download the data stored in a given HYCOM netcdf file for a single geographical point.

    Args:
        data_set_url (str): the url of the HYCOM's netcdf files
        lat (float): latitude of the point to be retrieved
        lon (float): longitude of the point to be retrieved
        requested_cols (list, optional): List of the columns to be downloaded, if you want to download only a subsample.

    Returns:
        pd.DataFrame: data for the specified location
    """
    print(f"Downloading {data_set_url}...\n")
    try:
        data = xr.open_dataset(data_set_url,
                               decode_times=False,
                               decode_cf=False)
    except:
        logging.warning(f"File {data_set_url} not found, skipping")
    if requested_cols is not None:
        return data[requested_cols].sel(lat=lat, lon=lon, method="nearest").to_dataframe()
    else:
        return data.sel(lat=lat, lon=lon, method="nearest").to_dataframe()


def download_multiple(url_list : list, lat : float, lon : float, requested_cols=None):
    """
    Download data for a given geographical point from a list of specified HYCOM's netcdf urls.

    Args:
        url_list (list): list of netcdf file urls to be considered
        lat (float): latitude of the point to be retrieved
        lon (float): longitude of the point to be retrieved
        requested_cols (list, optional): List of the columns to be downloaded, if you want to download only a subsample.

    Returns:
        pd.DataFrame: data for the specified location
    """
    download_lat_lon = partial(
        download, lat=lat, lon=lon, requested_cols=requested_cols)
    pool = Pool(multiprocessing.cpu_count()-1)
    list_data = pool.map(download_lat_lon, url_list)
    pool.close()
    return pd.concat(list_data).reset_index()


def download_data(from_date : datetime, to_date: datetime, lat: float, lon: float, requested_cols=None) -> pd.DataFrame:
    """
    This function is intended as the interface between the user and the library. 
    It takes as input the initial and final dates of the desired time range and
    the geographical coordinates of the requested point and (optionally) 
    the list of the feature to be downloaded, if you don't need the full dataset. 

    It returns the data as a pandas dataframe.

    Args:
        from_date (datetime): lower limit of the desired time range
        to_date (datetime): upper limit of the desired time range
        lat (float): latitude of the point to be retrieved
        lon (float): longitude of the point to be retrieved
        requested_cols (list, optional): List of the columns to be downloaded, if you want to download only a subsample.

    Returns:
        pd.DataFrame: data for the specified location
    """
    return download_multiple(get_url_list(from_date, to_date), lat, lon, requested_cols)
