# -*- coding: utf-8 -*-
"""
Tasks for br_denatran_frota
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

from prefect import task
import glob
import os
from pipelines.datasets.br_denatran_frota.constants import constants
from pipelines.datasets.br_denatran_frota.utils import (
    make_dir_when_not_exists,
    download_post_2012,
    verify_total,
    change_df_header,
    guess_header,
    get_year_month_from_filename,
)
import pandas as pd
import polars as pl

MONTHS = constants.MONTHS.value
DATASET = constants.DATASET.value
DICT_UFS = constants.DICT_UFS.value


@task(
    name="crawler", description="Extrai os arquivos do Denatran para ano e mês."
)  # noqa
def crawl(month: int, year: int, temp_dir: str = ""):
    """Função principal para baixar os dados de frota por município e tipo e também por UF e tipo.

    Args:
        month (int): Mês desejado.
        year (int): Ano desejado.

    Raises:
        ValueError: Errors if the month is not a valid one.
    """
    if month not in MONTHS.values():
        raise ValueError("Mês inválido.")

    dir_list = glob.glob(f"**/{DATASET}", recursive=True)
    # Get the directory where this Python file is located
    initial_dir = os.path.dirname(os.path.abspath(__file__))
    if temp_dir:
        os.chdir(temp_dir)
    # Construct the path to the "files" directory relative to this directory
    files_dir = os.path.join(os.getcwd(), "files")
    if dir_list:
        # I always want to be in the actual folder for this dataset, because I might start in the pipelines full repo:
        os.chdir(dir_list[0])

    # I always need a files directory inside my dataset folder.
    make_dir_when_not_exists(files_dir)
    # I should always switch to the files dir now and save stuff inside it.
    os.chdir(files_dir)
    year_dir_name = f"{year}"

    # Create dir for that specific year should it be necessary.
    make_dir_when_not_exists(year_dir_name)
    os.chdir(year_dir_name)
    if year > 2012:
        download_post_2012(month, year)
    # else:
    #     url = f"https://www.gov.br/infraestrutura/pt-br/assuntos/transito/arquivos-senatran/estatisticas/renavam/{year}/frota{'_' if year > 2008 else ''}{year}.zip"

    #     generic_zip_filename = f"geral_{year}.zip"
    #     urlretrieve(url, generic_zip_filename)
    #     with ZipFile(generic_zip_filename) as zip_file:
    #         zip_file.extractall()
    os.chdir(initial_dir)


@task(name="treatment of uf data", description="Trata os dados do Denatran para UF")
def treat_uf_tipo(file) -> pl.DataFrame:
    filename = os.path.split(file)[1]
    df = pd.read_excel(file)
    new_df = change_df_header(df, guess_header(df))
    # This is ad hoc for UF_tipo.
    new_df.rename(
        columns={new_df.columns[0]: "sigla_uf"}, inplace=True
    )  # Rename for ease of use.
    new_df.sigla_uf = new_df.sigla_uf.str.strip()  # Remove whitespace.
    clean_df = new_df[new_df.sigla_uf.isin(DICT_UFS.values())].reset_index(
        drop=True
    )  # Now we get all the actual RELEVANT uf data.
    month, year = get_year_month_from_filename(filename)
    clean_pl_df = pl.from_pandas(clean_df).lazy()
    verify_total(clean_pl_df.collect())
    # Add year and month
    clean_pl_df = clean_pl_df.with_columns(
        pl.lit(year, dtype=pl.Int64).alias("ano"),
        pl.lit(month, dtype=pl.Int64).alias("mes"),
    )
    clean_pl_df = clean_pl_df.select(pl.exclude("TOTAL"))
    clean_pl_df = clean_pl_df.melt(
        id_vars=["ano", "mes", "sigla_uf"],
        variable_name="tipo_veiculo",
        value_name="quantidade",
    )  # Long format.
    return clean_pl_df.collect()
