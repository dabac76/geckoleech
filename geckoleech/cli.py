import click
# import duckdb


# TODO: Clean database
# TODO: Init database (create model)
@click.group()
def cli():
    pass


@cli.command("db")
def db_report():
    click.echo("DB REPORT")


@cli.command("dry-run")
def dry_run():
    click.echo("RUN DRY")
