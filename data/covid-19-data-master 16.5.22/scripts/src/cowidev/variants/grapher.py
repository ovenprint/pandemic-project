import os
import pandas as pd

from cowidev import PATHS
from cowidev.grapher.files import Grapheriser, Exploriser


NUM_SEQUENCES_TOTAL_THRESHOLD = 100
FILE_GRAPHER = os.path.join(PATHS.INTERNAL_GRAPHER_DIR, "COVID-19 - Variants.csv")
FILE_SEQ_GRAPHER = os.path.join(PATHS.INTERNAL_GRAPHER_DIR, "COVID-19 - Sequencing.csv")
FILE_EXPLORER = os.path.join(PATHS.DATA_INTERNAL_DIR, "megafile--variants.json")


def filter_by_num_sequences(df: pd.DataFrame) -> pd.DataFrame:
    msk = df.num_sequences_total < NUM_SEQUENCES_TOTAL_THRESHOLD
    # Info
    _sk_perc_rows = round(100 * (msk.sum() / len(df)), 2)
    _sk_num_countries = df.loc[msk, "location"].nunique()
    _sk_countries_top = df[msk]["location"].value_counts().head(10).to_dict()
    print(
        f"Skipping {msk.sum()} datapoints ({_sk_perc_rows}%), affecting {_sk_num_countries} countries. Some are:"
        f" {_sk_countries_top}"
    )
    return df[~msk]


def run_grapheriser():
    # Variants
    Grapheriser(
        pivot_column="variant",
        pivot_values=["num_sequences", "perc_sequences"],
        fillna_0=True,
        function_input=filter_by_num_sequences,
        suffixes=["", "_percentage"],
    ).run(PATHS.INTERNAL_OUTPUT_VARIANTS_FILE, FILE_GRAPHER)
    # Sequencing
    Grapheriser(
        fillna_0=True,
    ).run(PATHS.INTERNAL_OUTPUT_VARIANTS_SEQ_FILE, FILE_SEQ_GRAPHER)


def run_explorerizer():
    Exploriser(
        pivot_column="variant",
        pivot_values="perc_sequences",
        function_input=filter_by_num_sequences,
    ).run(PATHS.INTERNAL_OUTPUT_VARIANTS_FILE, FILE_EXPLORER)


def run_db_updater(input_path: str):
    raise NotImplementedError("Not yet implemented")
