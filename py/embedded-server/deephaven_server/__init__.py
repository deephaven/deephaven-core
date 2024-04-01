#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import importlib.metadata
import click
import secrets
import signal
import sys
import webbrowser

from .server import Server

from deephaven_internal.jvm import check_py_env

check_py_env()

# Note: this is the _distribution_ name, not the _package_ name. Until 3.10, there is not an easy way to get the
# distribution name from the package name.
# https://docs.python.org/3/library/importlib.metadata.html#package-distributions
__version__ = importlib.metadata.version("deephaven-server")


@click.group()
def cli():
    """
    Command-line interface entry point for the Deephaven embedded server application.
    Accepts a command to start the application and the arguments to use.
    """
    pass


@cli.command()
@click.option("--jvm-args", default=None, help="The JVM arguments to use.")
def server(jvm_args):
    """
    Start the Deephaven server.
    """
    click.echo("Starting Deephaven server...")

    if jvm_args is None:
        jvm_args = ""
    jvm_args = jvm_args.split()

    s = Server(jvm_args=jvm_args)
    s.start()

    target_url_or_default = s.server_config.target_url_or_default
    authentication_type = None
    authentication_url = target_url_or_default
    for authentication_handler in s.authentication_handlers:
        authentication_urls = authentication_handler.urls(target_url_or_default)
        if authentication_urls:
            authentication_type = authentication_handler.auth_type
            authentication_url = authentication_urls[0]
            break

    webbrowser.open(authentication_url)

    click.echo(
        f"Deephaven is running at {authentication_url} with authentication type {authentication_type}"
    )
    click.echo("Press Control-C to exit")

    def signal_handler(sig, frame):
        click.echo("Exiting Deephaven...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.pause()
