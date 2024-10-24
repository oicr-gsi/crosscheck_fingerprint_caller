from crosscheck_fingerprint_caller import main
import os
import pandas


def test_load():
    df = main.load(
        "test/files/load_REVWGTS.29181.crosscheck_metrics.txt",
        "test/files/load_REVWGTS.29181.crosscheck_metrics.json",
    )
    gld_f = "test/files/load_REVWGTS.29181.crosscheck_metrics.csv"
    if not os.path.isfile(gld_f):
        df.to_csv(gld_f, index=False)

    golden = pandas.read_csv(gld_f)
    pandas.testing.assert_frame_equal(df, golden, check_like=True)


def test_ambiguous():
    df = pandas.DataFrame.from_dict(
        {
            "library_design": ["WG", "WG"],
            "library_design_match": ["WG", "WT"],
            "LOD_SCORE": [300.45, -100.45],
        }
    )
    out = main.is_ambiguous(df, "test/files/ambiguous_lod.json")
    assert (out == [False, True]).all()
