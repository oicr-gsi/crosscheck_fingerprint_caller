import argparse
import pandas
import json
import typing
from pandas import DataFrame


def main(args=None):
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

    parser.add_argument(
        "-c", "--output-calls", help="File path for the output swap calls"
    )
    parser.add_argument(
        "-d",
        "--output-detailed",
        help="File path for all called matches and swaps",
    )

    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)
    df = load(args.files, args.metadata)
    ambg = is_ambiguous(df, args.ambiguous_lod)
    swaps = is_swap(df, ambg)

    cols = [
        "run",
        "lane",
        "barcode",
        "donor",
        "external_donor_id",
        "library_name",
        "library_design",
        "tissue_type",
        "tissue_origin",
        "project",
        "lims_id",
    ]
    gen_call = generate_calls(df[cols], swaps)

    if args.output_calls is not None:
        gen_call.to_csv(args.output_calls, index=False)

    if args.output_detailed is not None:
        cols_match = [x + "_match" for x in cols]
        cols_match.append("LOD_SCORE")
        match = marked_match(df, ambg)
        generate_pairwise_calls(
            df[cols + cols_match], match, swaps, gen_call
        ).to_csv(args.output_detailed, index=False)


def generate_calls(df: DataFrame, swaps: pandas.Series) -> DataFrame:
    swaps = swaps.to_frame("pairwise_swaps")
    cols = list(df)
    df = pandas.merge(df, swaps, left_index=True, right_index=True)
    s = df.groupby(cols)["pairwise_swaps"].any()
    # noinspection PyTypeChecker
    return s.rename("swap_call", inplace=True).reset_index()


def generate_pairwise_calls(
    df: DataFrame, match: pandas.Series, swaps: pandas.Series, calls: DataFrame
) -> DataFrame:
    fltr = match | swaps
    df = df[fltr].copy()
    df["pairwise_swap"] = swaps[fltr]
    df["match_called"] = match[fltr]
    return pandas.merge(
        df,
        calls[["lims_id", "swap_call"]],
        how="left",
        left_on="lims_id",
        right_on="lims_id",
        validate="many_to_one",
    )


def load(fs: typing.List[str], metadata: str) -> DataFrame:
    with open(metadata, "r") as f:
        meta = json.load(f)
    meta = DataFrame.from_records(meta)

    inputs = []
    for f in fs:
        inputs.append(pandas.read_csv(f, sep="\t", comment="#"))

    df = pandas.concat(inputs, ignore_index=True)
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


def marked_match(df: DataFrame, ambg: pandas.Series) -> pandas.Series:
    keep = (df["LOD_SCORE"] > 0) & (~ambg)
    keep_ambg = (df["donor"] == df["donor_match"]) & ambg
    keep = keep | keep_ambg
    diff_lib = df["library_name"] != df["library_name_match"]
    keep = keep & diff_lib
    return keep


def same_batch(df: DataFrame) -> pandas.Series:
    def intrs(x):
        s = set(x["batches"]).intersection(x["batches_match"])
        return len(s) > 0

    return df.apply(intrs, axis=1)
