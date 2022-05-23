# type: ignore[attr-defined]

"""
Typer-based CLI for testing and experimentation
"""

import csv
import ujson as json
import logging
import os
from typing import List, Dict

from tabulate import tabulate
import typer

from firepit import get_storage


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


app = typer.Typer(
    name="firepit",
    help="Columnar storage for STIX observations",
    add_completion=False,
)


state = {
    'dbname': os.getenv('FIREPITDB', 'stix.db'),
    'session': os.getenv('FIREPITID', 'test-id'),
}


format_help = "Output format [table, json, csv]"


def print_rows(rows: List[Dict], format: str):
    if format == 'json':
        print(json.dumps(rows, ensure_ascii=False))  # , separators=[',', ':']))
    elif format == 'csv':
        for row in rows:
            print(','.join([json.dumps(item, ensure_ascii=False) for item in row.values()]))
    else:
        print(tabulate(rows, headers='keys'))


@app.callback()
def main(
    dbname: str = typer.Option(None, help="Path/name of database"),
    session: str = typer.Option(state['session'], help="Session ID to data separation"),
):
    if dbname:
        state['dbname'] = dbname
    if session:
        state['session'] = session


@app.command()
def cache(
    query_id: str = typer.Argument(..., help="An identifier for this set of data"),
    filenames: List[str] = typer.Argument(..., help="STIX bundle files of query results"),
    batchsize: int = typer.Option(2000, help="Max objects to insert per statement"),
):
    """Cache STIX observation data in SQL"""
    db = get_storage(state['dbname'], state['session'])
    if isinstance(filenames, tuple):
        filenames = list(filenames)
    db.cache(query_id, filenames, batchsize)


@app.command()
def extract(
    name: str = typer.Argument(..., help="Name for this new view"),
    sco_type: str = typer.Argument(..., help="SCO type to extract"),
    query_id: str = typer.Argument(..., help="Identifier for cached data to extract from"),
    pattern: str = typer.Argument(..., help="STIX pattern to filter cached data"),
):
    """Create a view of a subset of cached data"""
    db = get_storage(state['dbname'], state['session'])
    db.extract(name, sco_type, query_id, pattern)


@app.command()
def filter(
    name: str = typer.Argument(..., help="Name for this new view"),
    sco_type: str = typer.Argument(..., help="SCO type to extract"),
    source: str = typer.Argument(..., help="Source view"),
    pattern: str = typer.Argument(..., help="STIX pattern to filter cached data"),
):
    """Create a filtered view of a subset of cached data"""
    db = get_storage(state['dbname'], state['session'])
    db.filter(name, sco_type, source, pattern)


@app.command()
def assign(
    name: str = typer.Argument(..., help="Name for this new view"),
    view: str = typer.Argument(..., help="View name to operate on"),
    op: str = typer.Option(..., help="Operation to perform (sort, group, etc.)"),
    by: str = typer.Option(..., help="STIX object path"),
    desc: bool = typer.Option(False, help="Sort descending"),
    limit: int = typer.Option(None, help="Max number of rows to return"),
):
    """Perform an operation on a column and name the result"""
    db = get_storage(state['dbname'], state['session'])
    asc = not desc
    db.assign(name, view, op, by, asc, limit)


@app.command()
def join(
    name: str = typer.Argument(..., help="Name for this new view"),
    left_view: str = typer.Argument(..., help="Left view name to join"),
    left_on: str = typer.Argument(..., help="Column from left view to join on"),
    right_view: str = typer.Argument(..., help="Right view name to join"),
    right_on: str = typer.Argument(..., help="Column from right view to join on"),
):
    """Join two views"""
    db = get_storage(state['dbname'], state['session'])
    db.join(name, left_view, left_on, right_view, right_on)


@app.command()
def lookup(
    name: str = typer.Argument(..., help="View name to look up"),
    limit: int = typer.Option(None, help="Max number of rows to return"),
    offset: int = typer.Option(0, help="Number of rows to skip"),
    format: str = typer.Option('table', help=format_help),
    columns: str = typer.Option(None, help="List of columns to retrieve"),
):
    """Retrieve a view"""
    db = get_storage(state['dbname'], state['session'])
    if not columns:
        columns = '*'
    rows = db.lookup(name, cols=columns, limit=limit, offset=offset)
    print_rows(rows, format)


@app.command()
def values(
    path: str = typer.Argument(..., help="STIX object path to retrieve from view"),
    name: str = typer.Argument(..., help="View name to look up"),
):
    """Retrieve the values of a STIX object path from a view"""
    db = get_storage(state['dbname'], state['session'])
    rows = db.values(path, name)
    for row in rows:
        print(row)


@app.command()
def tables():
    """Get all view/table names"""
    db = get_storage(state['dbname'], state['session'])
    rows = db.tables()
    for row in rows:
        print(row)


@app.command()
def views():
    """Get all view names"""
    db = get_storage(state['dbname'], state['session'])
    rows = db.views()
    for row in rows:
        print(row)


@app.command()
def viewdata(
    views: List[str] = typer.Argument(None, help="Views to merge"),
    format: str = typer.Option('table', help=format_help),
):
    """Get view data for views [default is all views]"""
    db = get_storage(state['dbname'], state['session'])
    rows = db.get_view_data(views)
    print_rows(rows, format)


@app.command()
def columns(
    name: str = typer.Argument(..., help="View name to look up"),
):
    """Get the columns names of a view/table"""
    db = get_storage(state['dbname'], state['session'])
    rows = db.columns(name)
    for row in rows:
        print(row)


@app.command()
def type(
    name: str = typer.Argument(..., help="View name to look up"),
):
    """Get the SCO type of a view/table"""
    db = get_storage(state['dbname'], state['session'])
    print(db.table_type(name))


@app.command()
def schema(
    name: str = typer.Argument(..., help="View name to look up"),
):
    """Get the schema of a view/table"""
    db = get_storage(state['dbname'], state['session'])
    rows = db.schema(name)
    print(tabulate(rows, headers='keys'))


@app.command()
def count(
    name: str = typer.Argument(..., help="View name to look up"),
):
    """Get the count of rows in a view/table"""
    db = get_storage(state['dbname'], state['session'])
    print(db.count(name))


@app.command()
def delete(
):
    """Delete STIX observation data in SQL"""
    db = get_storage(state['dbname'], state['session'])
    db.delete()


@app.command()
def sql(
    statement: str = typer.Argument(..., help="View name to look up"),
):
    """Run a SQL statement on the database [DANGEROUS!]"""
    db = get_storage(state['dbname'], state['session'])
    rows = db._execute(statement)
    if rows:
        print(tabulate(rows, headers='keys'))


@app.command()
def set_appdata(
    name: str = typer.Argument(..., help="View name"),
    data: str = typer.Argument(..., help="Data (string)"),
):
    """Set the app-specific data for a view"""
    db = get_storage(state['dbname'], state['session'])
    db.set_appdata(name, data)


@app.command()
def get_appdata(
    name: str = typer.Argument(..., help="View name"),
):
    """Get the app-specific data for a view"""
    db = get_storage(state['dbname'], state['session'])
    print(db.get_appdata(name))


@app.command()
def load(
    name: str = typer.Argument(..., help="Name for this new view"),
    sco_type: str = typer.Option(None, help="SCO type of data to load"),
    query_id: str = typer.Option(None, help="An identifier for this set of data"),
    preserve_ids: str = typer.Option(True, help="Use IDs in the data"),
    filename: str = typer.Argument(..., help="Data file to load (JSON only)"),
):
    """Cache STIX observation data in SQL"""
    db = get_storage(state['dbname'], state['session'])
    try:
        with open(filename, 'r') as fp:
            data = json.load(fp)
    except ValueError:  # json.decoder.JSONDecodeError:
        with open(filename, 'r') as fp:
            data = list(csv.DictReader(fp))
    db.load(name, data, sco_type, query_id, preserve_ids)


@app.command()
def reassign(
    name: str = typer.Argument(..., help="Name for this new view"),
    filename: str = typer.Argument(..., help="Data file to load (JSON only)"),
):
    """Update/replace STIX observation data in SQL"""
    db = get_storage(state['dbname'], state['session'])
    with open(filename, 'r') as fp:
        data = json.load(fp)
    db.reassign(name, data)


@app.command()
def merge(
    name: str = typer.Argument(..., help="Name for this new view"),
    views: List[str] = typer.Argument(..., help="Views to merge"),
):
    """Merge 2 or more views into a new view"""
    db = get_storage(state['dbname'], state['session'])
    db.merge(name, views)


@app.command()
def remove(
    name: str = typer.Argument(..., help="Name of view to remove"),
):
    """Remove a view"""
    db = get_storage(state['dbname'], state['session'])
    db.remove_view(name)


@app.command()
def rename(
    oldname: str = typer.Argument(..., help="Name of view to rename"),
    newname: str = typer.Argument(..., help="New name of view to rename"),
):
    """Rename a view"""
    db = get_storage(state['dbname'], state['session'])
    db.rename_view(oldname, newname)


@app.command()
def value_counts(
    name: str = typer.Argument(..., help="View name to look up"),
    column: str = typer.Argument(..., help="Column to tabulate"),
    format: str = typer.Option('table', help=format_help),
):
    """Retrieve the value counts of a column from a view"""
    db = get_storage(state['dbname'], state['session'])
    rows = db.value_counts(name, column)
    print_rows(rows, format)


@app.command()
def number_observed(
    name: str = typer.Argument(..., help="View name to look up"),
    column: str = typer.Argument(..., help="Column to tabulate"),
    value: str = typer.Option(None, help="Column value to filter for"),
):
    """Retrieve the count of values of a column from a view"""
    db = get_storage(state['dbname'], state['session'])
    print(db.number_observed(name, column, value))


@app.command()
def timestamped(
    name: str = typer.Argument(..., help="View name to look up"),
    column: List[str] = typer.Argument(None, help="Column(s) to tabulate"),
    value: str = typer.Option(None, help="Column value to filter for"),
    timestamp: str = typer.Option('first_observed',
                                  help="Timestamp to use [first_observed, last_observed]"),
    limit: int = typer.Option(None, help="Max number of rows to return"),
    format: str = typer.Option('table', help=format_help),
):
    """Retrieve the timestamped values of a column from a view"""
    db = get_storage(state['dbname'], state['session'])
    rows = db.timestamped(name, column, value, timestamp, limit)
    print_rows(rows, format)


@app.command()
def summary(
    name: str = typer.Argument(..., help="View name to look up"),
    column: str = typer.Argument(None, help="Column to tabulate"),
    value: str = typer.Option(None, help="Column value to filter for"),
    format: str = typer.Option('table', help=format_help),
):
    """Retrieve timeframe and count from a view"""
    db = get_storage(state['dbname'], state['session'])
    row = db.summary(name, column, value)
    print_rows([row], format)


if __name__ == "__main__":
    app()
