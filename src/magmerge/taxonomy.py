def split_taxonomy(classif: str) -> dict:
    cols = {
        "Domain": None,
        "Phylum": None,
        "Class": None,
        "Order": None,
        "Family": None,
        "Genus": None,
        "Species": None,
    }
    if not isinstance(classif, str):
        return cols
    for token in classif.split(";"):
        if "__" not in token:
            continue
        prefix, name = token.split("__", 1)
        if prefix == "d":
            cols["Domain"] = name
        elif prefix == "p":
            cols["Phylum"] = name
        elif prefix == "c":
            cols["Class"] = name
        elif prefix == "o":
            cols["Order"] = name
        elif prefix == "f":
            cols["Family"] = name
        elif prefix == "g":
            cols["Genus"] = name
        elif prefix == "s":
            cols["Species"] = name
    return cols
