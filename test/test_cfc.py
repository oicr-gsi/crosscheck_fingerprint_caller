from crosscheck_fingerprint_caller import main
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

    golden = pandas.read_csv(
        gld_f,
    )
    pandas.testing.assert_frame_equal(df, golden, check_like=True)


def test_load_df():
    df = pandas.DataFrame.from_dict(
        {
            "LEFT_GROUP_VALUE": ["a", "a", "b", "b"],
            "RIGHT_GROUP_VALUE": ["a", "b", "a", "b"],
            "LOD_SCORE": [1, 2, 3, 4],
        }
    )

    metadata = pandas.DataFrame.from_dict(
        {"merge_key": ["a", "b"], "column": ["c", "d"]}
    )

    out = main.load_df(df, metadata)
    pandas.testing.assert_frame_equal(
        out,
        pandas.DataFrame.from_dict(
            {
                "LEFT_GROUP_VALUE": ["b", "b", "a", "a"],
                "RIGHT_GROUP_VALUE": ["b", "a", "b", "a"],
                "LOD_SCORE": [4, 3, 2, 1],
                "merge_key": ["b", "b", "a", "a"],
                "column": ["d", "d", "c", "c"],
                "merge_key_match": ["b", "a", "b", "a"],
                "column_match": ["d", "c", "d", "c"],
            }
        ),
        check_like=True,
    )

    metadata_missing = pandas.DataFrame.from_dict(
        {"merge_key": ["a"], "column": ["c"]}
    )
    out_missing = main.load_df(df, metadata_missing)
    pandas.testing.assert_frame_equal(
        out_missing,
        pandas.DataFrame.from_dict(
            {
                "LEFT_GROUP_VALUE": ["a"],
                "RIGHT_GROUP_VALUE": ["a"],
                "LOD_SCORE": [1],
                "merge_key": ["a"],
                "column": ["c"],
                "merge_key_match": ["a"],
                "column_match": ["c"],
            }
        ),
        check_like=True,
    )


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

    out = main.generate_calls(df, ["library_name", "library_design"], swaps)
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


def test_batch_overlap():
    df = pandas.DataFrame.from_dict(
        {
            "batches": ["", "", "1", "1,2"],
            "batches_match": ["", "1", "2", "1"],
        }
    )

    assert list(main.batch_overlap(df, ",")) == [set(), set(), set(), {"1"}]


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
    batch = pandas.Series([{"1"}, {"1", "2"}, {}, {}, {"3"}, {}])
    calls = pandas.DataFrame.from_dict(
        {"lims_id": ["a", "b", "c"], "swap_call": [False, True, False]}
    )

    out = main.generate_detailed_calls(df, match, swaps, batch, calls, ",")
    pandas.testing.assert_frame_equal(
        out,
        pandas.DataFrame.from_dict(
            {
                "lims_id": ["a", "a", "b", "b", "c"],
                "pairwise_swap": [False, False, False, True, False],
                "match_called": [True, True, True, True, True],
                "same_batch": [True, True, False, False, True],
                "overlap_batch": ["1", "1,2", "", "", "3"],
                "swap_call": [False, False, True, True, False],
            }
        ),
    )


def test_group_by_columns():
    df = pandas.DataFrame.from_dict(
        {
            "LOD_SCORE": [1],
            "non_hashable": [[1]],
            "merge_key": ["exclude"],
            "library_match": ["exclude"],
            "keep": [1],
        }
    )

    assert main.group_by_columns(df) == ["keep"]
