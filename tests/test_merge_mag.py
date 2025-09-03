import pandas as pd
import pytest

from loguru import logger

from magmerge.merge_mag import prepare_mag_table


def make_inputs_with_sampleid():
    # df_cov: coverage
    df_cov = pd.DataFrame(
        {
            "rname": ["c1", "c2", "c3"],
            "endpos": [100, 200, 300],
            "numreads": [10, 20, 30],
            "sample_id": ["S1", "S1", "S1"],
        }
    )

    # df_bin: contig -> bin + bin_score
    df_bin = pd.DataFrame(
        {
            "contig": ["c1", "c2", "c3"],
            "bin": ["bin1", "bin1", "bin2"],
            "bin_score": [50, 50, 80],
        }
    )


    df_gtdb = pd.DataFrame(
        {
            "user_genome": ["bin1", "bin2"],
            "classification": [
                "d__Bacteria;p__Firmicutes;c__Bacilli;o__OrderX;f__Fam;g__Genus;s__Species",
                "d__Bacteria;p__Proteobacteria;c__Gammaproteo;o__Enterobacterales;f__Enterobacteriaceae;g__Escherichia;s__coli",
            ],
            "closest_genome_reference": ["ref1", "ref2"],
            "closest_genome_ani": ["95.5", "99.9"],
        }
    )
    return df_gtdb, df_cov, df_bin


def test_happy_path_with_sampleid():
    df_gtdb, df_cov, df_bin = make_inputs_with_sampleid()

    out = prepare_mag_table(df_gtdb, df_cov, df_bin)

    # both bins present, no NaN values
    assert set(out["mag_id"]) == {"bin1", "bin2"}
    assert "genome_size" in out.columns
    assert "relative_abundance" in out.columns
    assert "bin_score" in out.columns
    # check taxonomy split
    assert out.loc[out["mag_id"] == "bin1", "Genus"].iloc[0] == "Genus"
    assert out.loc[out["mag_id"] == "bin2", "Genus"].iloc[0] == "Escherichia"


def test_without_bin_score_column():
    df_gtdb, df_cov, df_bin = make_inputs_with_sampleid()
    df_bin = df_bin.drop(columns=["bin_score"])  # remove bin_score

    out = prepare_mag_table(df_gtdb, df_cov, df_bin)
    assert "bin_score" in out.columns
    assert out.empty

def test_without_sampleid_column():
    df_gtdb, df_cov, df_bin = make_inputs_with_sampleid()
    df_cov = df_cov.drop(columns=["sample_id"])

    out = prepare_mag_table(df_gtdb, df_cov, df_bin)
    # relative_abundance still computed (no NaN)
    assert out["relative_abundance"].between(0, 1).all()
    assert set(out["mag_id"]) == {"bin1", "bin2"}


def test_invalid_ani_value_drops_record():
    df_gtdb, df_cov, df_bin = make_inputs_with_sampleid()
    # put non-numeric ANI for bin1 → converted to NaN → record removed
    df_gtdb.loc[df_gtdb["user_genome"] == "bin1", "closest_genome_ani"] = "not_a_number"

    messages = []
    sink = logger.add(lambda m: messages.append(m), level="INFO")

    out = prepare_mag_table(df_gtdb, df_cov, df_bin)

    logger.remove(sink)

    # only bin2 should remain
    assert set(out["mag_id"]) == {"bin2"}
    # log should mention how many deleted
    combined = "".join(m.record["message"] for m in messages)
    assert "Deleted" in combined


def test_empty_inputs_returns_empty_df():
    df_gtdb = pd.DataFrame(
        columns=["user_genome", "classification", "closest_genome_reference", "closest_genome_ani"]
    )
    df_cov = pd.DataFrame(columns=["rname", "endpos", "numreads"])
    df_bin = pd.DataFrame(columns=["contig", "bin", "bin_score"])

    # prepare_mag_table na kompletnie pustych danych nie ma jak stworzyć wszystkich kolumn → KeyError
    with pytest.raises(KeyError):
        _ = prepare_mag_table(df_gtdb, df_cov, df_bin)