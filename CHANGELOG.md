# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2025-04-14
### Changed
* split `load_df` from `load` to make testing easier
* rows are removed if metadata for them is missing (kept rows with `NaN` before)
* README improvements

## [0.3.0] - 2024-11-26
### Changed
* fixed bug where libraries not in batches would be marked to be in the same batch

## [0.2.0] - 2024-11-13
### Changed
* `batches` field changed from list to a string with a separator.
WDL and Shesmu enforce the same types within a `dict`, so the metadata JSON is now all string.

## [0.1.0] - 2024-11-08
### Added
* Changelog
* Readme
* Testing
* Working script
