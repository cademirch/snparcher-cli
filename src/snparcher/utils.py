import pandas as pd
from pathlib import Path


def parse_sample_sheet(csv_file: Path) -> pd.DataFrame:
    #!TODO validation n stuff. check out pandera
    df = pd.read_csv(csv_file)
    return df
