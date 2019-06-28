[![Build Status](https://cloud.drone.io/api/badges/cjohnhanson/schematic/status.svg)](https://cloud.drone.io/cjohnhanson/schematic)
# Schematic
Schematic is a set of utilities for transforming data among different data warehousing solutions.

It can infer a destination schema for arbitrary data from a source.

In its current version, it supports Redshift and CSV.

## Installation
Schematic is only supported for Python 3.5 and up.
You can install with pip:
`pip install git+https://github.com/cjohnhanson/schematic`

## CLI
Schematic can be used as a CLI utility. Once installed, just run `schematic` to get usage information.
```
$ schematic
Usage: schematic [OPTIONS] COMMAND [ARGS]...

  Utilities for converting data for tranfer among different data warehouse
  solutions

Options:
  --help  Show this message and exit.

Commands:
  create-table  Create a Redshift table from a CSV
```
You can also get help for each subcommand.
```
$ schematic create-table --help
Usage: schematic create-table [OPTIONS] CSV

  Create a Redshift table from a CSV

Options:
  --schema TEXT
  --conn-string TEXT  psycopg2-style connection string
  --help              Show this message and exit.
```
