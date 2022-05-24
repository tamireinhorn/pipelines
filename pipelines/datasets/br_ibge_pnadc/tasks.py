# -*- coding: utf-8 -*-
"""
Tasks for br_ibge_pnadc
"""
import os
import re
from io import BytesIO
from typing import Tuple
import requests
from collections import defaultdict

import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
from prefect import task


# pylint: disable=C0103
@task(nout=2)
def crawl(url: str) -> Tuple[str, str]:
    """
    Get all table urls from FTP url
    """
    html_page = requests.get(url).content
    soup = BeautifulSoup(html_page, parse_only=SoupStrainer('a'))
    
    years_to_extract = []
    year_pattern = re.compile(r'\d{4}/')

    for link in soup:
        direction = link.attrs['href']
        if year_pattern.fullmatch(direction):
            years_to_extract.append(direction)
    
    files_urls = defaultdict(list)
    for year in years_to_extract:
        year_url = url+year
        html = requests.get(year_url).text
        soup = BeautifulSoup(html,  parse_only=SoupStrainer('a'))
        for link in soup:
            direction = link.attrs['href']
            if 'PNADC' in direction:
                files_urls[year].append(year_url+direction)
    files_urls = dict(files_urls)
    
    
    # regex_pattern = re.compile(r"\d{6}")
    # temporal_coverage = regex_pattern.findall("".join(urls))
    # temporal_coverage.sort()
    # start_date = temporal_coverage[0][:4] + "-" + temporal_coverage[0][4:]
    # end_date = temporal_coverage[-1][:4] + "-" + temporal_coverage[-1][4:]
    # temporal_coverage = start_date + "(4)" + end_date

    return files_urls


# pylint: disable=C0103
@task
def clean_save_table(root: str, url_list: list):
    """Standardizes column names and selected variables"""
    path_out = f"{root}/terceirizados.csv"
    os.makedirs(root, exist_ok=True)

    

    df.to_csv(path_out, encoding="utf-8", index=False)

    return path_out
