from crosscheck_fingerprint_caller import main
import json
import os
import pandas
import shutil
import tempfile


def test_load():
    df = main.load(
        ["test/files/load_REVWGTS.29181.crosscheck_metrics.txt"],
        "test/files/load_REVWGTS.29181.crosscheck_metrics.json",
    )
    gld_f = "test/files/load_REVWGTS.29181.crosscheck_metrics.csv"
    if not os.path.isfile(gld_f):
        df.to_csv(gld_f, index=False)

    # Dealing with batches being a JSON list saved in a CSV file
    # First, convert the CSV `'` to `"` and then explicitly load the string as a list
    golden = pandas.read_csv(
        gld_f,
        converters={
            "batches": lambda x: json.loads(x.replace("'", '"')),
            "batches_match": lambda x: json.loads(x.replace("'", '"')),
        },
    )
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

    out = main.is_ambiguous(df, None)
    assert out.eq([False, False]).all()


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


def test_marked_match():
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
    out = main.mark_match(df, ambg)
    assert list(out) == [False, True, True, False, True, False]


def test_generate_calls():
    df = pandas.DataFrame.from_dict(
        {
            "library_name": ["1", "1", "2", "2"],
            "library_design": ["WG", "WG", "WG", "WG"],
        }
    )
    swaps = pandas.Series([False, True, False, False])

    out = main.generate_calls(df, swaps)
    pandas.testing.assert_frame_equal(
        out,
        pandas.DataFrame.from_dict(
            {
                "library_name": ["1", "2"],
                "library_design": ["WG", "WG"],
                "swap_call": [True, False],
            }
        ),
    )


def test_output_calls():
    with tempfile.TemporaryDirectory() as test_dir:
        output = os.path.join(test_dir, "output_caller.csv")
        main.main(
            [
                "-a",
                "test/files/ambiguous_lod.json",
                "-c",
                output,
                "test/files/load_REVWGTS.29181.crosscheck_metrics.json",
                "test/files/load_REVWGTS.29181.crosscheck_metrics.txt",
            ]
        )

        gld_f = "test/files/output_caller_golden.csv"
        if not os.path.isfile(gld_f):
            shutil.copyfile(output, gld_f)

        pandas.testing.assert_frame_equal(
            pandas.read_csv(output), pandas.read_csv(gld_f)
        )


def test_output_detailed():
    with tempfile.TemporaryDirectory() as test_dir:
        output = os.path.join(test_dir, "output_detailed.csv")
        main.main(
            [
                "-a",
                "test/files/ambiguous_lod.json",
                "-d",
                output,
                "test/files/load_REVWGTS.29181.crosscheck_metrics.json",
                "test/files/load_REVWGTS.29181.crosscheck_metrics.txt",
            ]
        )

        gld_f = "test/files/output_detailed_golden.csv"
        if not os.path.isfile(gld_f):
            shutil.copyfile(output, gld_f)

        pandas.testing.assert_frame_equal(
            pandas.read_csv(output), pandas.read_csv(gld_f)
        )


def test_same_batch():
    df = pandas.DataFrame.from_dict(
        {
            "batches": [[], [1], [1, 2]],
            "batches_match": [[1], [2], [1]],
        }
    )

    assert list(main.same_batch(df)) == [False, False, True]


def test_generate_pairwise_calls():
    # 0a: Match is called and no swap
    # 1a: Match is called and no swap
    # 2b: Match is called and no swap
    # 3b: Match is called and swap
    # 4c: Match is called and no swap
    # 5c: No match is called and no swap

    # The last element is ignored as there is no match or swap
    df = pandas.DataFrame.from_dict({"lims_id": ["a", "a", "b", "b", "c", "c"]})
    match = pandas.Series([True, True, True, True, True, False])
    swaps = pandas.Series([False, False, False, True, False, False])
    batch = pandas.Series([True, True, False, False, True, False])
    calls = pandas.DataFrame.from_dict(
        {"lims_id": ["a", "b", "c"], "swap_call": [False, True, False]}
    )

    out = main.generate_detailed_calls(df, match, swaps, batch, calls)
    pandas.testing.assert_frame_equal(
        out,
        pandas.DataFrame.from_dict(
            {
                "lims_id": ["a", "a", "b", "b", "c"],
                "pairwise_swap": [False, False, False, True, False],
                "match_called": [True, True, True, True, True],
                "same_batch": [True, True, False, False, True],
                "swap_call": [False, False, True, True, False],
            }
        ),
    )
