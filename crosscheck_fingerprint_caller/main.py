import argparse


def main():
    parser = argparse.ArgumentParser(
        prog="crosscheck_fingerprint_caller",
        description="Call swaps from CrosscheckFingerprint output",
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
        "-i",
        "--ignore-library",
        help="JSON file containing a list of known false positive libraries to ignore for swap calling",
    )

    parser.add_argument(
        "-p",
        "--ignore-pair",
        help="JSON file containing a list of pairs, each of which is a known false positive pair to ignore for swap calling",
    )

    parser.parse_args()
