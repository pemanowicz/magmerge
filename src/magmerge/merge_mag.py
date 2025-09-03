import pandas as pd
from loguru import logger

from src.magmerge.taxonomy import split_taxonomy


def prepare_mag_table(
    df_gtdb: pd.DataFrame, df_cov: pd.DataFrame, df_bin: pd.DataFrame
) -> pd.DataFrame:
    """
    Builds the final MAG table as required.
    Expected inputs:
    - df_gtdb: columns at least ['user_genome','classification','closest_genome_reference','closest_genome_ani']
    - df_cov: columns at least ['rname','endpos','numreads'] (+ optional 'sample_id')
    - df_bin: columns at least ['contig','bin'] + (from DASTool_summary.tsv) 'bin_score'
    Returns a DataFrame with columns:
    ['mag_id','genome_size','bin_score','relative_abundance',
    'Domain','Phylum','Class','Order','Family','Genus','Species', 'closest_reference_genome_id','closest_reference_genome_ani']
    and prints how many records were rejected due to missing values.
    """

    # 1) map contig->bin and connect to coverage
    # sanity dtype
    cov = df_cov.copy()
    cov.columns = [str(c).lstrip("#") for c in cov.columns]
    cov["endpos"] = pd.to_numeric(cov["endpos"], errors="coerce")
    cov["numreads"] = pd.to_numeric(cov["numreads"], errors="coerce")

    contig2bin = df_bin[["contig", "bin"]].dropna().copy()

    cov_bin = cov.merge(contig2bin, left_on="rname", right_on="contig", how="inner")

    # 2) genome_size: sum of contig lengths in the bin
    # I take the contig length as endpos (coverage counted from 1 to endpos)
    contig_len = cov_bin.groupby(["bin", "rname"], as_index=False)[
        "endpos"
    ].max()  # na wypadek duplikatów rname w pliku
    genome_size = (
        contig_len.groupby("bin", as_index=False)["endpos"]
        .sum()
        .rename(columns={"bin": "mag_id", "endpos": "genome_size"})
    )
    # 3) relative abundance: share of readings per bin
    if "sample_id" in cov_bin.columns:
        reads_per = (
            cov_bin.groupby(["sample_id", "bin"], as_index=False)["numreads"]
            .sum()
            .rename(columns={"numreads": "reads_in_bin"})
        )
        total_reads = (
            cov_bin.groupby("sample_id", as_index=False)["numreads"]
            .sum()
            .rename(columns={"numreads": "reads_total"})
        )
        rel = reads_per.merge(total_reads, on="sample_id", how="left")
        rel["relative_abundance"] = rel["reads_in_bin"] / rel["reads_total"]
        rel = rel.rename(columns={"bin": "mag_id"})[["mag_id", "relative_abundance"]]
        # If I have multiple samples, duplicate mag_ids from different samples may result.
        # Consolidate by sum (or average). By default, I'll take the sum of the contributions (typically 1 sample => no influence).
        rel = rel.groupby("mag_id", as_index=False)["relative_abundance"].sum()
    else:
        reads_per = (
            cov_bin.groupby("bin", as_index=False)["numreads"]
            .sum()
            .rename(columns={"numreads": "reads_in_bin"})
        )
        total_reads = reads_per["reads_in_bin"].sum()
        rel = reads_per.assign(relative_abundance=reads_per["reads_in_bin"] / total_reads)
        rel = rel.rename(columns={"bin": "mag_id"})[["mag_id", "relative_abundance"]]

    # 4) bin_score from DASTool_summary
    # Take unique bin_score per bin (sometimes repeated per contig).
    if "bin_score" in df_bin.columns:
        bs = df_bin[["bin", "bin_score"]].dropna(subset=["bin"]).drop_duplicates(subset=["bin"])
        bs["bin_score"] = pd.to_numeric(bs["bin_score"], errors="coerce")
        bs = bs.rename(columns={"bin": "mag_id"})
    else:
        # if no column in input
        bs = pd.DataFrame(columns=["mag_id", "bin_score"])

    # 5) GTDB: taxonomy + closest genome
    gtdb = df_gtdb[
        ["user_genome", "classification", "closest_genome_reference", "closest_genome_ani"]
    ].copy()

    print(gtdb["classification"])

    tax = gtdb["classification"].apply(split_taxonomy).apply(pd.Series)
    print(tax)

    gtdb_clean = pd.concat([gtdb.drop(columns=["classification"]), tax], axis=1)
    gtdb_clean = gtdb_clean.rename(
        columns={
            "user_genome": "mag_id",
            "closest_genome_reference": "closest_reference_genome_id",
            "closest_genome_ani": "closest_reference_genome_ani",
        }
    )
    gtdb_clean["closest_reference_genome_ani"] = pd.to_numeric(
        gtdb_clean["closest_reference_genome_ani"], errors="coerce"
    )

    # 6) Merging everything by mag_id
    merged = (
        genome_size.merge(rel, on="mag_id", how="left")
        .merge(bs, on="mag_id", how="left")
        .merge(gtdb_clean, on="mag_id", how="left")
    )

    # 7) First select only the required columns
    wanted = [
        "mag_id",
        "genome_size",
        "bin_score",
        "relative_abundance",
        "Domain",
        "Phylum",
        "Class",
        "Order",
        "Family",
        "Genus",
        "Species",
        "closest_reference_genome_id",
        "closest_reference_genome_ani",
    ]
    out = merged[wanted].copy()

    # Now remove missing records and report
    before = len(out)
    out_clean = out.dropna()
    removed = before - len(out_clean)
    logger.info(f"Deleted {removed} from {before} records with missind data (NaN/NULL).")

    return out_clean
