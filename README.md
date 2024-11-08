# crosscheck_fingerprint_caller

## Purpose

To call sample swaps given
[CrosscheckFingerprints](https://gatk.broadinstitute.org/hc/en-us/articles/360037594711-CrosscheckFingerprints-Picard)
and OICR metadata.

CrosscheckFingerprints is designed to call swaps, but two of its limitations prevent that for OICR:
1. Grouping samples from the same individual can only be done from the header of the input VCF files.
OICR does not encode donor information there.
2. There is a single LOD cutoff. If LOD is above, samples are expected to match.
The single cutoff works poorly for libraries other than Whole Genome.
These libraries tend to have an LOD randomly distributed around an LOD score of 0.
OICR needs to pick a range for LOD cutoffs and those ranges need to depend on library type input.

## Terms

* Ambiguous LOD: an inclusive range that includes an LOD score of 0
* Donor: an individual, a patient.
* Library: sequenced and aligned to generate the BAM file
* Library Design: two letter for library: whole genome (WG), whole transcriptome (WT), etc
* Match: the caller calculates that two libraries are expected to come from the same donor
* Swap: the donors aren't equal when a match was called or the donors are equal when a match was not called

## Design

### OICR Metadata

OICR metadata is expected to be in a JSON file ([example](doc/metadata_example.json)).

The mandatory fields are:
* donor: used to detect swaps
* library_name: used to exclude libraries matching themselves
* library_design: to assign ambiguous LOD range
* lims_id: unique key
* merge_key: link OICR metadata to the CrosscheckFingerprints
`LEFT_GROUP_VALUE` and `RIGHT_GROUP_VALUE` columns
* batches: list of batches the library is in

The mandatory fields, except merge_key and batches, are included in the output.
Additional fields are allowed and will be added to the output.

### Ambiguous Range

Ambiguous range is defined for each library design in a JSON file ([example](doc/ambiguous_lod_example.json)).

LOD calls outside the ambiguous range behave same as CrosscheckFingerprints.
Libraries from the same donor are expected to have positive LOD and negative LOD if from different donors.
If that expectation is broken, a swap is called.

LOD within the ambiguous range is more permissive for calling matches.
Libraries with a positive LOD that don't match are not called as a swap.
Libraries with a negative LOD that do match are not called as a swap and marked as a expected match.

## Usage
```commandline
crosscheck-fingerprint-caller --help

usage: crosscheck_fingerprint_caller [-h] [-a AMBIGUOUS_LOD] [-c OUTPUT_CALLS] [-d OUTPUT_DETAILED] [-s SEPERATOR] metadata files [files ...]

Call swaps from CrosscheckFingerprint output

positional arguments:
  metadata              JSON file of data to merge into CrosscheckFingerprint files
  files                 One or more CrosscheckFingerprint files

options:
  -h, --help            show this help message and exit
  -a, --ambiguous-lod AMBIGUOUS_LOD
                        JSON file describing what LOD range is inconclusive for a given library design pairing
  -c, --output-calls OUTPUT_CALLS
                        File path for the output swap calls
  -d, --output-detailed OUTPUT_DETAILED
                        File path for all called matches and swaps
  -s, --seperator SEPERATOR
                        The seperator to use for turning lists into strings (default `,`)
```

## Output

### Call File
The call file contains one row for each unique `lims_id`. OICR metadata is preserved.
A `swap_call` column is added that is `True` if swap is called for that library, `False` otherwise.

### Detailed File
Contains a row for each library pair which are a match and/or a swap.

OICR metadata is preserved.
`_match` string is appended to query library columns to distinguish them from query library columns.

The following columns are added:
* pairwise_swap: is this row a swap
* match_called: is this row a match
* same_batch: did the library pair share at least one batch
* overlap_batch: which batches did the libraries share
* swap_call: the swap call for the query library