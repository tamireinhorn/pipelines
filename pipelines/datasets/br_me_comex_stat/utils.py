# -*- coding: utf-8 -*-
""" Utils for the Brazilian Comex Stat pipeline. """
# pylint: disable=invalid-name
import os
import requests

from tqdm import tqdm
from pipelines.utils.utils import (
    log,
)


def create_paths(path):
    """
    Create and partition folders
    """
    path_temps = [path, path + "input/", path + "output/"]

    for path_temp in path_temps:
        os.makedirs(path_temp, exist_ok=True)


def download_data(
    path: str,
    table_type: str,
    table_name: str,
) -> None:
    """
    Crawler for br_me_comex_stat
    """
    # todo: create a way to automatically feed inputs here

    log(f"Downloading {table_type} of {table_name}")
    url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{table_type}/{table_name}.zip"
    log(f" da {url}")
    r = requests.get(url, verify=False, timeout=99999999)
    with open(path + f"input/{table_name}.zip", "wb") as f:
        f.write(r.content)


'''
def download_data(path):
    """
    Crawler for br_me_comex_stat
    """
    groups = {
        "ncm": ["EXP_COMPLETA", "IMP_COMPLETA"],
        "mun": ["EXP_COMPLETA_MUN", "IMP_COMPLETA_MUN"],
    }

    for item, value in groups.items():
        for group in tqdm(value):
            log(f"Downloading {item} of {group}")
            url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/{item}/{group}.zip"
            r = requests.get(url, verify=False, timeout=99999999)
            with open(path + f"input/{group}.zip", "wb") as f:
                f.write(r.content)
'''
