import pandas as pd
import pytest
from pathlib import Path
from loguru import logger

import magmerge.pipelines as pl


def write_paths_csv(tmp_path: Path, rows: list[dict]) -> Path:
    df = pd.DataFrame(rows)
    p = tmp_path / "python_paths.csv"
    df.to_csv(p, index=False)
    return p


def test_pipeline_binning_merges_contig2bin_and_summary(tmp_path):
    # create input files
    folder = tmp_path / "s1"
    folder.mkdir()
    (folder / "S1_DASTool_contig2bin.tsv").write_text("contigA\tbin1\ncontigB\tbin2\n")
    (folder / "S1_DASTool_summary.tsv").write_text("bin\tscore\nbin1\t100\nbin2\t200\n")

    paths_csv = write_paths_csv(
        tmp_path, [{"study_id": "st1", "sample_id": "S1", "stage": "BINNING", "folder": str(folder)}]
    )

    df = pl.pipeline_Binning(str(paths_csv), print_paths=False)

    # two contigs with matching summary info
    assert set(df.columns) >= {"contig", "bin", "score"}
    assert len(df) == 2
    row = df[df["contig"] == "contigA"].iloc[0]
    assert row["bin"] == "bin1"
    assert row["score"] == "100"


def test_pipeline_binning_missing_files_logs_warning(tmp_path):
    folder = tmp_path / "s2"
    folder.mkdir()
    # only contig2bin file
    (folder / "S2_DASTool_contig2bin.tsv").write_text("contigX\tbin9\n")

    paths_csv = write_paths_csv(
        tmp_path,
        [{"study_id": "st2", "sample_id": "S2", "stage": "BINNING", "folder": str(folder)}],
    )

    # capture loguru warnings
    messages = []
    sink_id = logger.add(lambda m: messages.append(m), level="WARNING")

    df = pl.pipeline_Binning(str(paths_csv), print_paths=False)

    logger.remove(sink_id)

    # contig2bin loaded, summary missing → still returns DF
    assert "contigX" in df["contig"].tolist()

    # warning should be logged
    all_msgs = "".join(m.record["message"] for m in messages)
    assert "File not found" in all_msgs



def test_pipeline_binning_empty_when_no_rows(tmp_path):
    paths_csv = write_paths_csv(
        tmp_path, [{"study_id": "st3", "sample_id": "S3", "stage": "COVERAGE", "folder": "x"}]
    )
    df = pl.pipeline_Binning(str(paths_csv), print_paths=False)
    assert df.empty


def test_pipeline_coverage_reads_and_strips_hash(tmp_path):
    folder = tmp_path / "s4"
    folder.mkdir()
    (folder / "S4_coverage.tsv").write_text("#rname\treads\ncontigA\t10\n")
    paths_csv = write_paths_csv(
        tmp_path, [{"study_id": "st4", "sample_id": "S4", "stage": "COVERAGE", "folder": str(folder)}]
    )

    df = pl.pipeline_COVERAGE(str(paths_csv), print_paths=False)

    # header should have hash stripped
    assert "rname" in df.columns
    assert "reads" in df.columns
    assert df.loc[0, "rname"] == "contigA"
    assert df.loc[0, "reads"] == "10"


def test_pipeline_gtdbtk_reads_summary(tmp_path):
    folder = tmp_path / "s5"
    folder.mkdir()
    (folder / "gtdbtk.bac120.summary.tsv").write_text("user_genome\ttaxonomy\nMAG1\td__Bacteria;p__Firmicutes\n")
    paths_csv = write_paths_csv(
        tmp_path, [{"study_id": "st5", "sample_id": "S5", "stage": "GTDBTK", "folder": str(folder)}]
    )

    df = pl.pipeline_GTDBTK(str(paths_csv), print_paths=False)

    assert "user_genome" in df.columns
    assert "taxonomy" in df.columns
    assert df.iloc[0]["user_genome"] == "MAG1"
    assert "Bacteria" in df.iloc[0]["taxonomy"]
