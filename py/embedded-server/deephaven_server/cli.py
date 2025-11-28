#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
import sys
import webbrowser
from typing import List, Optional

import click

from .server import Server


@click.group()
def cli() -> None:
    """
    Command-line interface entry point for the Deephaven embedded server application.
    Accepts a command to start the application and the arguments to use.
    """
    pass


@cli.command()
@click.option("--host", default=None, help="The host to bind to.")
@click.option("--port", default=None, type=int, help="The port to bind to.")
@click.option("--jvm-args", default=None, help="The JVM arguments to use.")
@click.option("--extra-classpath", default=None, help="The extra classpath to use.")
@click.option(
    "--default-jvm-args",
    default=None,
    help="The advanced JVM arguments to use in place of the default ones that Deephaven recommends.",
)
@click.option(
    "--browser/--no-browser",
    default=True,
    help="Whether to open the browser when the server starts.",
)
def server(
    host: Optional[str] = None,
    port: Optional[int] = None,
    jvm_args: Optional[str] = None,
    extra_classpath: Optional[str] = None,
    default_jvm_args: Optional[str] = None,
    browser: bool = True,
) -> None:
    """
    Start the Deephaven server.
    """
    click.echo("Starting Deephaven server...")

    jvm_args_l: Optional[List[str]] = jvm_args.split() if jvm_args else None
    default_jvm_args_l: Optional[List[str]] = (
        default_jvm_args.split() if default_jvm_args else None
    )
    extra_classpath_l: Optional[List[str]] = (
        extra_classpath.split() if extra_classpath else None
    )

    s = Server(
        host=host,
        port=port,
        jvm_args=jvm_args_l,
        extra_classpath=extra_classpath_l,
        default_jvm_args=default_jvm_args_l,
    )
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

    if browser:
        webbrowser.open(authentication_url)

    click.echo(
        f"Deephaven is running at {authentication_url} with authentication type {authentication_type}"
    )
    click.echo("Press Control-C to exit")

    try:
        while True:
            input()
    except KeyboardInterrupt:
        # signal_handler should already be called
        click.echo("Exiting Deephaven...")
        sys.exit(0)
