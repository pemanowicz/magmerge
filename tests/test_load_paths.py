import pandas as pd
from pathlib import Path
from loguru import logger
from magmerge.load_paths import load_stage_files


def write_paths_csv(tmp_path: Path, rows: list[dict]) -> Path:
    df = pd.DataFrame(rows)
    p = tmp_path / "python_paths.csv"
    df.to_csv(p, index=False)
    return p


def test_concatenates_frames_and_prints_paths(tmp_path, capsys):
    # CSV with two rows for the BINNING stage
    paths_csv = write_paths_csv(
        tmp_path,
        [
            {"study_id": "S1", "sample_id": "A", "stage": "BINNING", "folder": "A_dir"},
            {"study_id": "S1", "sample_id": "B", "stage": "BINNING", "folder": "B_dir"},
        ],
    )

    # build two deterministic paths (one per row)
    def build_paths_fn(row: pd.Series):
        return [tmp_path / row["folder"] / f"{row['sample_id']}_file.tsv"]

    # reader returns a tiny DF per file
    def reader_fn(p: Path):
        return pd.DataFrame({"sample_id": [p.stem.split("_")[0]], "path": [str(p)]})

    out = load_stage_files(
        paths_csv=str(paths_csv),
        stage="BINNING",
        build_paths_fn=build_paths_fn,
        reader_fn=reader_fn,
        print_paths=True,
    )

    # Expect two rows concatenated
    assert isinstance(out, pd.DataFrame)
    assert set(out["sample_id"]) == {"A", "B"}

    # Paths are printed when print_paths=True
    captured = capsys.readouterr().out
    assert "A_dir/A_file.tsv" in captured.replace("\\", "/")
    assert "B_dir/B_file.tsv" in captured.replace("\\", "/")


def test_filters_by_stage_only(tmp_path):
    paths_csv = write_paths_csv(
        tmp_path,
        [
            {"study_id": "S1", "sample_id": "A", "stage": "BINNING", "folder": "A_dir"},
            {"study_id": "S1", "sample_id": "B", "stage": "COVERAGE", "folder": "B_dir"},
        ],
    )

    def build_paths_fn(row: pd.Series):
        # Should only be called for stage==BINNING rows
        return [tmp_path / row["folder"] / f"{row['sample_id']}.tsv"]

    def reader_fn(p: Path):
        return pd.DataFrame({"p": [str(p)]})

    out = load_stage_files(
        paths_csv=str(paths_csv),
        stage="BINNING",
        build_paths_fn=build_paths_fn,
        reader_fn=reader_fn,
        print_paths=False,
    )

    # Only one row (the BINNING one) should be processed
    assert len(out) == 1
    assert "A_dir/A.tsv" in out.iloc[0]["p"].replace("\\", "/")


def test_missing_files_are_skipped_and_no_frames_returns_empty_df(tmp_path):
    paths_csv = write_paths_csv(
        tmp_path,
        [{"study_id": "S1", "sample_id": "A", "stage": "BINNING", "folder": "A_dir"}],
    )

    def build_paths_fn(row: pd.Series):
        return [tmp_path / "A_dir" / "missing.tsv"]

    def reader_fn(p: Path):
        # Simulate missing file
        raise FileNotFoundError(p)

    # Capture loguru logs
    messages = []
    sink_id = logger.add(lambda m: messages.append(m), level="WARNING")

    out = load_stage_files(
        paths_csv=str(paths_csv),
        stage="BINNING",
        build_paths_fn=build_paths_fn,
        reader_fn=reader_fn,
        print_paths=False,
    )

    logger.remove(sink_id)

    # Should return an empty DataFrame if every file is missing
    assert isinstance(out, pd.DataFrame)
    assert out.empty

    # Warning about missing file should be logged
    joined = "".join(m.record["message"] for m in messages)
    assert "File not found:" in joined


def test_reader_errors_are_logged_and_processing_continues(tmp_path):
    paths_csv = write_paths_csv(
        tmp_path,
        [{"study_id": "S1", "sample_id": "A", "stage": "BINNING", "folder": "A_dir"}],
    )

    bad = tmp_path / "A_dir" / "bad.tsv"
    good = tmp_path / "A_dir" / "good.tsv"

    def build_paths_fn(row: pd.Series):
        # Two inputs for the same row: first will error, second will succeed
        return [bad, good]

    def reader_fn(p: Path):
        if p == bad:
            raise ValueError("Corrupt file")
        # Successful read
        return pd.DataFrame({"src": [str(p)], "val": [1]})

    # Capture error logs
    errors = []
    sink_id = logger.add(lambda m: errors.append(m), level="ERROR")

    out = load_stage_files(
        paths_csv=str(paths_csv),
        stage="BINNING",
        build_paths_fn=build_paths_fn,
        reader_fn=reader_fn,
        print_paths=False,
    )

    logger.remove(sink_id)

    # Should contain only the successful frame
    assert len(out) == 1
    assert out.iloc[0]["val"] == 1
    assert "good.tsv" in out.iloc[0]["src"].replace("\\", "/")

    # Error message logged
    logged = "".join(m.record["message"] for m in errors)
    assert "Error reading file" in logged
    assert "bad.tsv" in logged


def test_print_paths_false_suppresses_stdout(tmp_path, capsys):
    paths_csv = write_paths_csv(
        tmp_path,
        [{"study_id": "S1", "sample_id": "A", "stage": "BINNING", "folder": "A_dir"}],
    )

    def build_paths_fn(row: pd.Series):
        return [tmp_path / "A_dir" / "x.tsv"]

    def reader_fn(p: Path):
        return pd.DataFrame({"ok": [True]})

    _ = load_stage_files(
        paths_csv=str(paths_csv),
        stage="BINNING",
        build_paths_fn=build_paths_fn,
        reader_fn=reader_fn,
        print_paths=False,
    )

    captured = capsys.readouterr()
    assert captured.out == ""
