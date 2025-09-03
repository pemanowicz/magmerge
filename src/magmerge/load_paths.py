import pandas as pd
from loguru import logger


def load_stage_files(
    paths_csv: str, stage: str, build_paths_fn, reader_fn, print_paths: bool = True
) -> pd.DataFrame:
    """
        Universal loader for files referenced in `python_paths.csv`.

    :param paths_csv: CSV file with columns: study_id, sample_id, stage, folder
    :param stage: name of the stage (BINNING, COVERAGE, GTDBTK)
    :param build_paths_fn: function (row: pd.Series) -> List[Path]
    :param reader_fn: function (Path) -> pd.DataFrame (may return an empty DataFrame if the file is missing)
    :param print_paths: whether to print the file paths
    :return: concatenated DataFrame

    """
    df_paths = pd.read_csv(
        paths_csv,
        sep=",",
        dtype=str,
    )
    rows = df_paths[df_paths["stage"] == stage].copy()
    frames: list[pd.DataFrame] = []

    for _, row in rows.iterrows():
        for path in build_paths_fn(row):
            if print_paths:
                print(path)
            try:
                df_part = reader_fn(path)
                frames.append(df_part)
            except FileNotFoundError:
                logger.warning(f"File not found: {path}")
            except Exception as e:
                logger.error(f"Error reading file {path}: {e}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
