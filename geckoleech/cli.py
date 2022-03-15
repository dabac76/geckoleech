import click
import os
import importlib.util
from geckoleech.utils import DuckDB, expand
""" Invoke with: $gecko <command> [options | --help]

    'gecko' prefix is exposed to command line in pyproject.toml 
"""


@click.group()
def cli():
    pass


@cli.command("db-ddl")
@click.option("-f", "--file", help="Path to file containing sql query.",
              type=click.Path(exists=True))
@click.option("-s", "--stmt", help="SQL statement to pass to geckoleech.db")
def db_ddl(file, stmt):
    with DuckDB() as ddb:
        if file:
            with open(file, "r") as f:
                sql = f.read()
        elif stmt:
            sql = stmt
        else:
            click.echo(f"Type --help for usage instructions.")
        ddb.execute(sql)


@cli.command("db-remove")
@click.option("-n", "--names", help="Delete all rows from tables with given names (quoted, space delimited). "
                                    "If no names given, delete from all tables in database.")
def db_remove(names: str):
    with DuckDB() as ddb:
        ddb.execute("PRAGMA show_tables;")
        tables = ddb.fetchall()
        if not names:
            for table in tables:
                ddb.execute("delete from " + table[0])
        else:
            db_names = [table[0] for table in tables]
            cli_names = names.split()
            if not all(cli_name in db_names for cli_name in cli_names):
                for cli_name in cli_names:
                    if not(cli_name in db_names):
                        click.echo(f"ERROR: Unknown table name: {cli_name}")
                return
            for cli_name in cli_names:
                ddb.execute("delete from " + cli_name)


@cli.command("db-stat")
def db_stat():
    with DuckDB(read_only=True) as ddb:
        ddb.execute("PRAGMA show_tables;")
        tables = ddb.fetchall()
        for tbl in tables:
            click.echo("-" * 60)
            click.echo(f"TABLE: {tbl[0]}")
            ddb.execute("PRAGMA table_info(" + tbl[0] + ");")
            tb_info = ddb.fetchall()
            ddb.execute("select count(*) from " + tbl[0] + ";")
            rows = ddb.fetchone()
            click.echo(f"ROWS : {rows[0]}")
            click.echo("-" * 60)
            for col in tb_info:
                click.echo(f"{col[1]:<16} {col[2]:>16} \t notnull: {col[3]:<5}")
        click.echo("-" * 60)
        # As of DuckDB 0.3.2. this still gives SEGMENTATION FAULT
        # ddb.execute("PRAGMA database_size;")
        # size = ddb.fetchall()
        # print(f"Database total_blocks: {size[2]}")
        # print(f"Database memory_usage: {size[6]}")
        # print("-" * 60)


@cli.command("dry-run")
@click.option("-f", "--file", help="Path to user script containing APIReq objects. "
                                   "Inside script guard leech() with: if __name__==\"__main__\" before calling this.",
              type=click.Path(exists=True))
def dry_run(file):
    spec = importlib.util.spec_from_file_location("userscript", os.path.abspath(file))
    userscript = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(userscript)
    # noinspection PyUnresolvedReferences
    for req in userscript.APIReq.all():
        click.echo("-" * 60)
        click.echo(f"Request name: {req.name}")
        click.echo("Called with :")
        for args_kwargs in expand(req.params):
            if not args_kwargs:
                args_kwargs = "()"
            click.echo(args_kwargs)
    click.echo("-" * 60)
