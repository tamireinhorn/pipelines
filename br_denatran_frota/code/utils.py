# -*- coding: utf-8 -*-
import pandas as pd
import polars as pl
from string_utils import asciify
from thefuzz import process
import re
from br_denatran_frota.code.constants import DICT_UFS, SUBSTITUTIONS


def guess_header(df: pd.DataFrame, max_header_guess: int = 4) -> int:
    """Function to deal with problematic dataframes coming from Excel/CSV files.
    Tries to guess which row is the header by examining the first few rows (max given by max_header_guess).
    Assumes that the header is the first row where all of the columns are strings.
    This will NOT work for a strings only dataframe.

    Args:
        df (pd.DataFrame): Dataframe whose header we don't know.
        max_header_guess (int, optional): Number of initial rows we want to check . Defaults to 4.

    Returns:
        int: Index of the row where the header is contained.
    """
    header_guess = 0
    while header_guess < max_header_guess:
        # Iffy logic, but essentially: if all rows of the column are strings, then this is a good candidate for a header.
        if all(df.iloc[header_guess].apply(lambda x: isinstance(x, str))):
            return header_guess

        header_guess += 1
    return 0  # If nothing is ever found until the max, let's just assume it's the first row as per usual.


def change_df_header(df: pd.DataFrame, header_row: int) -> pd.DataFrame:
    """Changes the dataframe's header to a row inside of it and returns the corrected df.
    Ideally, to be used in conjunction with guess_header().
    Args:
        df (pd.DataFrame): Dataframe whose header we want changed.
        header_row (int): Index of the row where the header is located.

    Returns:
        pd.DataFrame: Returns the same dataframe but with the corrected header
    """
    new_header = df.iloc[header_row]
    new_df = df[(header_row + 1) :].reset_index(drop=True)
    new_df.rename(columns=new_header, inplace=True)
    return new_df


def get_year_month_from_filename(filename: str) -> tuple[int, int]:
    """Helper to extract month and year information from files named indicator_month-year.xls

    Args:
        filename (str): Name of the file.

    Raises:
        ValueError: Errors out if nothing is found, which likely means the filename is not the correct format.

    Returns:
        tuple[int, int]: Month, year.
    """
    match = re.search(r"(\w+)_(\d{1,2})-(\d{4})\.(xls|xlsx)$", filename)
    if match:
        month = match.group(2)
        year = match.group(3)
        return month, year
    else:
        raise ValueError("No match found")


def verify_total(df: pl.DataFrame) -> None:
    """Helper function that is meant to act as a guard to guarantee that we can pivot from wide to long.
    Essentially, gets a Wide dataframe, excludes all string columns and the TOTAL column and sums it all row wise.
    Then verifies if the calculated total column is the same as the TOTAL column.
    If not, raises an error.

    Args:
        df (pl.DataFrame): Wide format dataframe to verify.

    Raises:
        ValueError: Errors out if the sum of the columns is actually different than the total.
    """
    columns_for_total = df.select(pl.exclude("TOTAL")).select(pl.exclude([pl.Utf8]))
    calculated_total = columns_for_total.select(
        pl.fold(
            acc=pl.lit(0), function=lambda acc, x: acc + x, exprs=pl.col("*")
        ).alias("calculated_total")
    )["calculated_total"]
    mask = df["TOTAL"] == calculated_total
    if pl.sum(~mask) != 0:
        raise ValueError(
            "A coluna de TOTAL da base original tem inconsistências e não é igual à soma das demais colunas."
        )


def fix_suggested_nome_ibge(row: tuple[str, ...]) -> str:
    """Gets a row from a dataframe and applies the SUBSTITUTIONS constant ruleset where applicable.
    This fixes the dataframe to have the names of municipalities according to IBGE.
    The fixes are necessary because match_ibge() will fail where the DENATRAN data has typos in city names.

    Args:
        row (tuple[str, ...]): Row from the full DENATRAN dataframe we want to apply the IBGE substitutions to.

    Raises:
        ValueError: Errors out if the desired parts of the row do not conform to the expected format.

    Returns:
        str: Returns the suggested IBGE name, either the pre existing or the one in the ruleset for substitutions.
    """
    key = (row[0], row[1])
    if (
        (not isinstance(row[0], str))
        or (not isinstance(row[1], str))
        or (not isinstance(row[-1], str))
    ):
        raise ValueError("This row is not in the expected format.")
    if key in SUBSTITUTIONS:
        return SUBSTITUTIONS[key]
    else:
        return row[-1]


def match_ibge(denatran_uf: pl.DataFrame, ibge_uf: pl.DataFrame) -> None:
    """Takes a dataframe of the Denatran data and an IBGE dataframe of municipalities.
    Joins them using the IBGE name of both. The IBGE name of denatran_uf is ideally filled via get_city_name().
    This verifies if there are any municipalities in denatran_uf that have no corresponding municipality in the IBGE database.
    These must be manually fixed via the constants file and the fix_suggested_nome function.
    Will error out if there are municipalities that have no correspondence whatsover.
    Args:
        denatran_uf (pl.DataFrame): Dataframe with the DENATRAN data, filtered by state (UF).
        ibge_uf (pl.DataFrame): Dataframe with the IBGE municipalities data, filtered by state (UF).

    Raises:
        ValueError: Errors if there are municipalities that have not match and outputs them all with the state to enable manual fix.
    """
    joined_df = denatran_uf.join(
        ibge_uf,
        left_on=["suggested_nome_ibge", "sigla_uf"],
        right_on=["nome", "sigla_uf"],
        how="left",
    )
    mismatched_rows = joined_df.filter(pl.col("id_municipio").is_null())
    if len(mismatched_rows) > 0:
        error_message = "Os seguintes municípios falharam: \n"
        for row in mismatched_rows.rows(named=True):
            error_message += f"{row['nome_denatran']} ({row['sigla_uf']})\n"
        raise ValueError(error_message)


def get_city_name_ibge(denatran_name: str, ibge_uf: pl.DataFrame) -> str:
    """Gets the closest match to the denatran name of the municipality in the IBGE dataframe of the same state.
    This ibge_uf dataframe is pulled directly from Base dos Dados to ensure correctness.
    Returns either the match or an empty string in case no match is found.


    Args:
        denatran_name (str): The name of the municipality according to DENATRAN data.
        ibge_uf (pl.DataFrame): Dataframe with the information from municipalities for a certain state (UF).
    Returns:
        str: Closest match to the denatran name or an empty string if no such match is found.
    """
    matches = process.extract(
        denatran_name.lower(),
        ibge_uf["nome"].apply(asciify).str.to_lowercase(),
        limit=1,
    )
    matched_name = matches[0]
    if matched_name[1] >= 60:  # Apply threshold
        return matched_name[0]
    else:
        return ""  # I don't want this to error out directly, because then I can get all municipalities.
