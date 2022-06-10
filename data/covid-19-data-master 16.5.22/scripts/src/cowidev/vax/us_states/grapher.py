from datetime import datetime

from cowidev.grapher.files import Grapheriser, Exploriser


def run_grapheriser(input_path: str, output_path: str):
    Grapheriser(
        fillna_0=False,
        date_ref=datetime(2021, 1, 1),
    ).run(input_path, output_path)


def run_explorerizer(input_path: str, output_path: str):
    raise NotImplementedError("Not implemented")


def run_db_updater(input_path: str):
    raise NotImplementedError("Not yet implemented")
