import click
from geckoleech.utils import DuckDB
""" Invoke with: $gecko <command> [options | --help]

    'gecko' prefix is exposed to command line in pyproject.toml 
"""


# TODO: Clean database
# TODO: Init database (create model)
# TODO: Report status in database
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


@cli.command("db-stat")
def db_stat():
    with DuckDB as ddb:
        ddb.execute("PRAGMA show_tables;")
        tbls = ddb.fetchall()
        for tbl in tbls:
            print("-"*20)
            print(tbl)
            print("-" * 20)
            ddb.execute("PRAGMA table_info(" + tbl + ");")
            tb_info = ddb.fetchall()
            ddb.execute("SELECT * FROM " + tbl + ";")
            rows = ddb.fetchone()
            print("-" * 20)
            print(f"Rows: {rows[0]}")
            print("-" * 20)
            for col in tb_info:
                print(f"{col[1]}: {col[2]} \t notnull: {col[3]}")
        print("-" * 20)
        ddb.execute("PRAGMA database_size;")
        size = ddb.fetchall()
        print(f"Database total_blocks: {size[2]}")
        print(f"Database memory_usage: {size[6]}")
        print("-" * 20)


@cli.command("dry-run")
def dry_run():
    click.echo("RUN DRY")
