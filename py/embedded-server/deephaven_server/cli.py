import click
import signal
import sys
import webbrowser

from .server import Server

@click.group()
def cli():
    """
    Command-line interface entry point for the Deephaven embedded server application.
    Accepts a command to start the application and the arguments to use.
    """
    pass


@cli.command()
@click.option("--host", default=None, help="The host to bind to.")
@click.option("--port", default=None, type=int, help="The port to bind to.")
@click.option(
    "--key",
    default=None,
    help="Start the server in pre-shared key mode. If the key is empty, a random key will be used. Cannot be used if anonymous is specified.",
)
@click.option(
    "--anonymous",
    is_flag=True,
    help="Start the server in anonymous mode. Cannot be used if key is specified.",
)
@click.option("--jvm-args", default=None, help="The JVM arguments to use.")
def server(host, port, key, anonymous, jvm_args):
    """
    Start the Deephaven server.
    """
    click.echo("Starting Deephaven server...")

    if anonymous and key is not None:
        raise click.ClickException("Cannot specify both --anonymous and --key")

    if jvm_args is None:
        jvm_args = ""
    jvm_args = jvm_args.split()

    if anonymous:
        jvm_args += [f"-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"]
    elif key is not None:
        jvm_args += [
            f"-DAuthHandlers=io.deephaven.authentication.psk.PskAuthenticationHandler"
        ]
        # We are relying on server to set random key otherwise
        if key:
            jvm_args += [f"-Dauthentication.psk={key}"]

    s = Server(host=host, port=port, jvm_args=jvm_args)
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