import pytest
from magmerge.taxonomy import split_taxonomy


def test_full_classification():
    classif = "d__Bacteria;p__Firmicutes;c__Bacilli;o__Lactobacillales;" \
              "f__Lactobacillaceae;g__Lactobacillus;s__Lactobacillus_acidophilus"
    result = split_taxonomy(classif)
    assert result["Domain"] == "Bacteria"
    assert result["Phylum"] == "Firmicutes"
    assert result["Class"] == "Bacilli"
    assert result["Order"] == "Lactobacillales"
    assert result["Family"] == "Lactobacillaceae"
    assert result["Genus"] == "Lactobacillus"
    assert result["Species"] == "Lactobacillus_acidophilus"


def test_partial_classification():
    classif = "d__Bacteria;p__Proteobacteria;g__Escherichia"
    result = split_taxonomy(classif)
    assert result["Domain"] == "Bacteria"
    assert result["Phylum"] == "Proteobacteria"
    assert result["Genus"] == "Escherichia"
    # levels not present should stay None
    assert result["Class"] is None
    assert result["Species"] is None


def test_non_string_input_returns_all_none():
    for bad in [None, 123, 3.14, ["d__Bacteria"]]:
        result = split_taxonomy(bad)
        assert all(v is None for v in result.values())


def test_tokens_without_double_underscore_are_skipped():
    classif = "d__Bacteria;BadToken;p__Firmicutes"
    result = split_taxonomy(classif)
    assert result["Domain"] == "Bacteria"
    assert result["Phylum"] == "Firmicutes"
    # BadToken should not break parsing
    assert all(k in result for k in ["Domain", "Phylum", "Class"])


def test_repeated_prefix_overwrites_value():
    classif = "d__Bacteria;d__Archaea"
    result = split_taxonomy(classif)
    # last occurrence wins
    assert result["Domain"] == "Archaea"


def test_empty_string_returns_all_none():
    result = split_taxonomy("")
    assert all(v is None for v in result.values())
