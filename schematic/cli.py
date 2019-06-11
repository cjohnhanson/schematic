# -*- coding: utf-8 -*-
# MIT License
#
# Copyright (c) 2019 Cody J. Hanson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import click
import psycopg2
from schematic.schematics import redshift_schematic, csv_schematic


@click.group()
def cli():
    """Utilities for converting data for tranfer among different data warehouse solutions"""


@cli.command()
@click.option("--schema")
@click.argument("csv", type=click.Path(exists=True))
@click.option("--conn-string", help="psycopg2-style connection string")
def create_table(schema, csv, conn_string):
    """Create a table from a CSV"""
    with open(csv) as csv_file:
        csv_table_def = csv_schematic.CSVTableDefinition.from_csv(csv_file)
        click.echo("Scanning CSV to determine types...")
        redshift_table_def = redshift_schematic.RedshiftSchematic().table_def_from_rows(
            schema=schema,
            name=csv_table_def.name,
            fieldnames=csv_table_def.column_names(),
            rows=csv_table_def.get_rows())
    click.echo("Creating table in Redshift...")
    with psycopg2.connect(conn_string) as connection:
        redshift_table_def.create_table(connection)
        connection.commit()
    click.secho("Successfully created table {}.{}".format(schema, csv_table_def.name),
                    fg="green")
