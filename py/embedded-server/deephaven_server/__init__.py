#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import importlib.metadata
import click
import secrets
import signal
import sys
import webbrowser

from .start_jvm import DEFAULT_JVM_PROPERTIES, DEFAULT_JVM_ARGS, start_jvm
from .server import Server

from deephaven_internal.jvm import check_py_env
check_py_env()

# Note: this is the _distribution_ name, not the _package_ name. Until 3.10, there is not an easy way to get the
# distribution name from the package name.
# https://docs.python.org/3/library/importlib.metadata.html#package-distributions
__version__ = importlib.metadata.version('deephaven-server')


@click.group()
def cli():
    """
    Command-line interface entry point for the Deephaven embedded server application.
    Accepts a command to start the application and the arguments to use.
    """
    pass

@cli.command()
@click.option("--host", default="localhost", help="The host to bind to. Defaults to localhost.")
@click.option("--port", default=None, type=int, help="The port to bind to.")
@click.option("--key", default=None, help="The key to use. Uses a randomly generated key if not specified.")
@click.option("--anonymous", is_flag=True, help="Start the server in anonymous mode. Cannot be used if key is specified.")
@click.option("--jvm-args", default=None, help="The JVM arguments to use.")
def server(host, port, key, anonymous, jvm_args):
    """
    Start the Deephaven server.
    """
    click.echo("Starting Deephaven server...")

    if anonymous and key is not None:
        raise click.ClickException("Cannot specify both --anonymous and --key")

    if jvm_args is None:
        jvm_args = ''
    jvm_args = jvm_args.split()

    if anonymous:
        jvm_args += [f"-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"]
    else:
        if key is None:
            key = secrets.token_urlsafe(32)
        jvm_args += [f"-Dauthentication.psk={key}"]

    s = Server(host=host, port=port, jvm_args=jvm_args)
    s.start()

    url = f"http://{host}:{s.port}/ide"

    if key is not None:
        url += f"?psk={key}"

    webbrowser.open(url)

    click.echo(f"Deephaven is running at {url}")
    click.echo("Press Control-C to exit")

    def signal_handler(sig, frame):
        click.echo("Exiting Deephaven...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.pause()