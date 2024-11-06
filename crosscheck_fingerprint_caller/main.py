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
        match = mark_match(df, ambg)
        sm_btch = same_batch(df)
        generate_detailed_calls(
            df[cols + cols_match], match, swaps, sm_btch, gen_call
        ).to_csv(args.output_detailed, index=False)


def generate_calls(df: DataFrame, swaps: pandas.Series) -> DataFrame:
    """
    Determine if at least one swap call occurred in an arbitrary grouping.
    The grouping is all the columns in the input DataFrame.
    The index of the DataFrame and the swap Series must match.

    Args:
        df: DataFrame that includes the columns to group by
        swaps: A index matches Series stating if a swap happened in that index

    Returns: Returns a new DataFrame with all columns from input DataFrame plus the `swap_call` column

    """
    swaps = swaps.to_frame("pairwise_swaps")
    cols = list(df)
    df = pandas.merge(df, swaps, left_index=True, right_index=True)
    s = df.groupby(cols)["pairwise_swaps"].any()
    # noinspection PyTypeChecker
    return s.rename("swap_call", inplace=True).reset_index()


def generate_detailed_calls(
    df: DataFrame,
    match: pandas.Series,
    swaps: pandas.Series,
    in_same_batch: pandas.Series,
    calls: DataFrame,
) -> DataFrame:
    """
    Return all DataFrame rows that are called as a match and/or are a swap.
    The following columns are added:
        * `pairwise_swap`: Is the library pair marked as a swap
        * `match_called`: Is the library pair called as a match
        * `same_batch`: Does the library pair have at least one batch in common
        * `swap_call`: Has the left library of the pair been marked as being involved in a swap

    Args:
        df: DataFrame that must contain the `lims_id` column
        match: Series of rows that have been marked as a match
        swaps: Series of rows that have been marked as a swap
        in_same_batch: Series of rows that have been marked as sharing at least one batch
        calls: DataFrame linking the `lims_id` to being involved in a swa

    Returns:

    """
    fltr = match | swaps
    df = df[fltr].copy()
    df["pairwise_swap"] = swaps[fltr]
    df["match_called"] = match[fltr]
    df["same_batch"] = in_same_batch[fltr]
    return pandas.merge(
        df,
        calls[["lims_id", "swap_call"]],
        how="left",
        left_on="lims_id",
        right_on="lims_id",
        validate="many_to_one",
    )


def load(fs: typing.List[str], metadata: str) -> DataFrame:
    """
    Combine the CrosscheckFingerprint file with the metadata JSON.
    The `LEFT_GROUP_VALUE` and `RIGHT_GROUP_VALUE` are merged with the `merge_key`.

    Args:
        fs: File path to the CrosscheckFingerprint file
        metadata: File path to the JSON metadata file

    Returns: The merged DataFrame

    """
    with open(metadata, "r") as f:
        meta = json.load(f)
    meta = DataFrame.from_records(meta)

    inputs = []
    for f in fs:
        inputs.append(
            pandas.read_csv(
                f,
                sep="\t",
                comment="#",
                usecols=["LEFT_GROUP_VALUE", "RIGHT_GROUP_VALUE", "LOD_SCORE"],
            )
        )

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
    """
    A library pair is ambiguous if the LOD score falls between a range that includes 0.
    Ambiguous ranges are set for each library design pairing.
    If a library design pairing is not provided, the default ambiguous range is 0.

    Args:
        df: DataFrame that includes `LOD_SCORE`, `library_design`, and `library_design_match`
        ambg: Path to JSON file containing inclusive ambiguous ranges

    Returns:

    """
    if ambg is None:
        j = []
    else:
        with open(ambg, "r") as f:
            j = json.load(f)

    d = {frozenset(x["pair"]): [x["upper"], x["lower"]] for x in j}

    result = []
    for _, r in df.iterrows():
        k = frozenset([r["library_design"], r["library_design_match"]])
        up, low = d.get(k, [0, 0])
        result.append(up >= r["LOD_SCORE"] >= low)

    return pandas.Series(result)


def is_swap(df: DataFrame, ambg: pandas.Series) -> pandas.Series:
    """
    A swap is called when all of these are false for the library pair:
        * the LOD is ambiguous
        * the donors match and LOD > 0
        * the donors don't match and LOD < 0
    Args:
        df: DataFrame must have `LOD_SCORE`, `donor`, and `donor_match`
        ambg: bool Series stating if the library pair is ambiguous

    Returns: bool Series stating if the pair is a swap

    """
    expected_match: pandas.Series = (df["LOD_SCORE"] > 0) & (
        df["donor"] == df["donor_match"]
    )
    expected_mismatch: pandas.Series = (df["LOD_SCORE"] < 0) & (
        df["donor"] != df["donor_match"]
    )
    not_swap = ambg | expected_match | expected_mismatch
    return ~not_swap


def mark_match(df: DataFrame, ambg: pandas.Series) -> pandas.Series:
    """
    Matched libraries means the algorithm thinks they come from the same patient.
    This function does not check if a swap has occurred.
    Matched libraries are:
        * Have a positive LOD score outside the ambiguous range OR
        * Come from the same donor within the ambiguous range

    Args:
        df: DataFrame that must contain `donor` and `library_name` fields and the `_matched` counterpart
        ambg: A Series of bool stating if the library pair is ambiguous or not

    Returns: A Series of bool stating if the library pair is a match

    """
    keep = (df["LOD_SCORE"] > 0) & (~ambg)
    keep_ambg = (df["donor"] == df["donor_match"]) & ambg
    keep = keep | keep_ambg
    diff_lib = df["library_name"] != df["library_name_match"]
    keep = keep & diff_lib
    return keep


def same_batch(df: DataFrame) -> pandas.Series:
    """
    Do the query and match library appear together in at least one batch.

    If they do, then the swap could be internal.

    Args:
        df: The DataFrame must contain the `batches` and `batches_match` columns.

    Returns: A Series of booleans stating if the library pair have at least one batch in common

    """

    def intrs(x):
        s = set(x["batches"]).intersection(x["batches_match"])
        return len(s) > 0

    return df.apply(intrs, axis=1)
