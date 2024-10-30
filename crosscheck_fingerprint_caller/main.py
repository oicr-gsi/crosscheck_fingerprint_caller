import argparse
import pandas
import json
import typing
from pandas import DataFrame


def main():
    parser = argparse.ArgumentParser(
        prog="crosscheck_fingerprint_caller",
        description="Call swaps from CrosscheckFingerprint output",
    )
    parser.add_argument(
        "metadata",
        help="JSON file of data to merge into CrosscheckFingerprint files",
    )

    parser.add_argument(
        "files", nargs="+", help="One or more CrosscheckFingerprint files"
    )

    parser.add_argument(
        "-a",
        "--ambiguous-lod",
        help="JSON file describing what LOD range is inconclusive for a given library design pairing",
    )

    parser.parse_args()


def load(f: str, metadata: str) -> DataFrame:
    df = pandas.read_csv(f, sep="\t", comment="#")
    with open(metadata, "r") as f:
        meta = json.load(f)
    meta = DataFrame.from_records(meta)
    df = df.merge(
        meta,
        how="left",
        left_on="LEFT_GROUP_VALUE",
        right_on="merge_key",
        validate="many_to_one",
    )
    df = df.merge(
        meta,
        how="left",
        left_on="RIGHT_GROUP_VALUE",
        right_on="merge_key",
        suffixes=(None, "_match"),
        validate="many_to_one",
    )
    df.sort_values(
        ["LEFT_GROUP_VALUE", "LOD_SCORE"], inplace=True, ascending=False
    )
    df.reset_index(inplace=True)
    return df


def is_ambiguous(df: DataFrame, ambg: typing.Optional[str]) -> pandas.Series:
    if ambg is None:
        j = []
    else:
        with open(ambg, "r") as f:
            j = json.load(f)

    d = dict()
    for i in j:
        d[frozenset(i["pair"])] = [i["upper"], i["lower"]]

    result = []
    for _, r in df.iterrows():
        k = frozenset([r["library_design"], r["library_design_match"]])
        up, low = d.get(k, [0, 0])
        result.append(up >= r["LOD_SCORE"] >= low)

    return pandas.Series(result)


def is_swap(df: DataFrame, ambg: pandas.Series) -> pandas.Series:
    result = []
    for i, r in df.iterrows():
        if ambg[i]:
            result.append(False)
        elif r["LOD_SCORE"] > 0 and r["donor"] == r["donor_match"]:
            result.append(False)
        elif r["LOD_SCORE"] < 0 and r["donor"] != r["donor_match"]:
            result.append(False)
        else:
            result.append(True)
    return pandas.Series(result)


def closest_lib(
    df: DataFrame, lib_name: str, ambg: pandas.Series
) -> pandas.Index:
    if len(df) < 2:
        pandas.Series([])

    df = df[df["library_name"] == lib_name]
    df = df[df["library_name_match"] != lib_name]
    index = []
    for i, r in df.iterrows():
        if ambg[i]:
            # Matches in the ambiguous range are good enough to stop. Ignore non-matches.
            if r["donor"] == r["donor_match"]:
                index.append(i)
                break
        else:
            index.append(i)
            if r["donor"] == r["donor_match"]:
                break

    return pandas.Index(index)


def graph_edges(df: DataFrame, ambg: pandas.Series) -> pandas.Index:
    keep = (df["LOD_SCORE"] > 0) & (~ambg)
    keep_ambg = (df["donor"] == df["donor_match"]) & ambg
    keep = keep | keep_ambg
    df = df[keep]
    df = df[df["library_name"] != df["library_name_match"]]
    return df.index
