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
    assert out.eq([False, True]).all()


def test_swap():
    df = pandas.DataFrame.from_dict(
        {
            "donor": [1, 1, 1, 1],
            "donor_match": [1, 2, 2, 2],
            "LOD_SCORE": [10, 10, 5, -10],
        }
    )
    ambg = pandas.Series([False, False, True, False])
    out = main.is_swap(df, ambg)
    # Expected match, expected mismatch, swap, swap by ambiguous (no not swap)
    assert out.eq([False, True, False, False]).all()


def test_closest_lib():
    # The first never matches because the two libraries are the same
    # The second match is from different donors
    # The third match stops as the same donor is found
    df = pandas.DataFrame.from_dict(
        {
            "library_name": ["1", "1", "1", "1"],
            "library_name_match": ["1", "2", "2", "2"],
            "donor": [1, 1, 1, 1],
            "donor_match": [1, 2, 1, 2],
        }
    )
    ambg = pandas.Series([False, False, False, False])
    out = main.closest_lib(df, "1", ambg)
    assert pandas.Index([1, 2]).equals(out)

    # Everything is ambiguous, so the second match is now ignored, as it's from a different donor
    ambg = pandas.Series([True, True, True, True])
    out = main.closest_lib(df, "1", ambg)
    assert pandas.Index([2]).equals(out)


def test_graph():
    # 0: not a node, as it comes from the same library (no self referencing)
    # 1: Node as positive LOD and not ambiguous (same donor)
    # 2: Node as positive LOD and not ambiguous (different donor)
    # 3: Not a node as positive LOD, but ambiguous and coming from a different donor
    # 4: Node as ambiguous (with negative LOD), but coming from the same donor
    # 5: Not a node as negative LOD and coming from a different donor
    df = pandas.DataFrame.from_dict(
        {
            "library_name": ["1", "1", "1", "1", "1", "1"],
            "library_name_match": ["1", "2", "2", "2", "2", "2"],
            "donor": [1, 1, 1, 1, 1, 1],
            "donor_match": [1, 1, 2, 2, 1, 2],
            "LOD_SCORE": [10, 10, 10, 2, -2, -10],
        }
    )
    ambg = pandas.Series([False, False, False, True, True, False])
    out = main.graph_edges(df, ambg)
    assert pandas.Index([1, 2, 4]).equals(out)
