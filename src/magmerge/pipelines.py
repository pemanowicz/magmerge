from pathlib import Path
import pandas as pd
from loguru import logger

from src.magmerge.load_paths import load_stage_files


# PIPELINE: BINNING
def pipeline_Binning(paths_csv: str, print_paths: bool = True) -> pd.DataFrame:
    def build_paths(row):
        folder = Path(row["folder"])
        sample_id = row["sample_id"]
        return [
            folder / f"{sample_id}_DASTool_contig2bin.tsv",
            folder / f"{sample_id}_DASTool_summary.tsv",
        ]

    def reader(path: Path) -> pd.DataFrame:
        if path.name.endswith("contig2bin.tsv"):
            return pd.read_csv(
                path,
                sep="\t",
                header=None,
                names=["contig", "bin"],
                dtype={"contig": "string", "bin": "string"},
            )
        elif path.name.endswith("summary.tsv"):
            return pd.read_csv(path, sep="\t", dtype="string")
        else:
            return pd.DataFrame()

    # special contig2bin join with summary → outer join after bin
    df_paths = pd.read_csv(
        paths_csv,
        sep=",",
        dtype=str,
    )
    bin_rows = df_paths[df_paths["stage"] == "BINNING"].copy()
    frames: list[pd.DataFrame] = []

    for _, row in bin_rows.iterrows():
        folder = Path(row["folder"])
        sample_id = row["sample_id"]
        contig2bin_path = folder / f"{sample_id}_DASTool_contig2bin.tsv"
        summary_path = folder / f"{sample_id}_DASTool_summary.tsv"

        if print_paths:
            logger.info(contig2bin_path)
            logger.info(summary_path)

        try:
            c2b = pd.read_csv(
                contig2bin_path, sep="\t", header=None, names=["contig", "bin"], dtype="string"
            )
        except FileNotFoundError:
            logger.warning(f"File not found: {contig2bin_path}")
            c2b = pd.DataFrame(columns=["contig", "bin"])

        try:
            summ = pd.read_csv(summary_path, sep="\t", dtype="string")
            if "bin" not in summ.columns:
                summ = pd.DataFrame(columns=["bin"])
        except FileNotFoundError:
            logger.warning(f"File not found: {summary_path}")
            summ = pd.DataFrame(columns=["bin"])

        merged = c2b.merge(summ, on="bin", how="outer")
        frames.append(merged)

    if not frames:
        return pd.DataFrame(columns=["contig", "bin"])

    return pd.concat(frames, ignore_index=True)


# PIPELINE: COVERAGE
def pipeline_COVERAGE(paths_csv: str, print_paths: bool = True) -> pd.DataFrame:
    def build_paths(row):
        folder = Path(row["folder"])
        sample_id = row["sample_id"]
        return [folder / f"{sample_id}_coverage.tsv"]

    def reader(path: Path) -> pd.DataFrame:
        df = pd.read_csv(path, sep="\t", dtype="string")
        df.columns = [col.lstrip("#") for col in df.columns]
        return df

    return load_stage_files(paths_csv, "COVERAGE", build_paths, reader, print_paths)


# PIPELINE: GTDBTK
def pipeline_GTDBTK(paths_csv: str, print_paths: bool = True) -> pd.DataFrame:
    def build_paths(row):
        folder = Path(row["folder"])
        return [folder / "gtdbtk.bac120.summary.tsv"]

    def reader(path: Path) -> pd.DataFrame:
        return pd.read_csv(path, sep="\t", dtype="string")

    return load_stage_files(paths_csv, "GTDBTK", build_paths, reader, print_paths)
