import click
import unittest
from scraping import TypeScrap, run as run_scraping

from project import app


@click.option(
    '--file',
    '-f',
    type=click.Path(),
    help='File path with tick naming list, default ./tickers.txt', default='./tickers.txt'
)
@click.option(
    '--restrict',
    '-r',
    type=TypeScrap,
    help='Type of the scraping on trades|prices, default all',
    default=TypeScrap.ALL
)
@click.option('--threads', '-t', type=int, help='Scraping threads count, between 1 and 10', default=5)
@app.cli.command()
def scraping(file, restrict, threads):
    run_scraping(file, threads, types_scrubs=restrict)


@app.cli.command()
def test():
    """Runs the unit tests."""
    tests = unittest.TestLoader().discover('tests')
    test_runner = unittest.TextTestRunner()
    test_runner.run(tests)


if __name__ == '__main__':
    app.run()
