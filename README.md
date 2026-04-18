# podlake

[![Tests](https://github.com/sul-dlss/podlake/actions/workflows/test.yml/badge.svg)](https://github.com/sul-dlss/podlake/actions/workflows/test.yml)

podlake is a command line utility for converting [POD](https://pod.stanford.edu/) MARC XML data into Parquet files and storing them in an Amazon S3 bucket for use in data analysis tools like DuckDB, Spark, Presto, or services like AWS Athena.

## Install

First install [uv] to get a working Python environment and then setup podlake 

```
$ uvx podlake config
```

You'll be prompted for your POD API key, S3 bucket and AWS credentials which will be saved.

Then you can harvest a single provider by name and write the output to a Parquet file:

```
$ uvx podlake convert stanford stanford.parquet
```

To harvest all providers at once and write each to its own Parquet file in a directory:

```
$ uvx podlake convert-all ./output/
```

By default this runs one worker at a time. Use `--workers` to harvest multiple providers concurrently:

```
$ uvx podlake convert-all ./output/ --workers 4
```

## Develop

You'll want to clone this repository, make changes and then run the tests:

```
$ uv run pytest
```

[POD]: https://pod.stanford.edu/
[uv]: https://docs.astral.sh/uv/
