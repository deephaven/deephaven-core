#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import importlib.metadata
import secrets
import signal
import sys
import argparse
import webbrowser

from .start_jvm import DEFAULT_JVM_PROPERTIES, DEFAULT_JVM_ARGS, start_jvm
from .server import Server

from deephaven_internal.jvm import check_py_env
check_py_env()

# Note: this is the _distribution_ name, not the _package_ name. Until 3.10, there is not an easy way to get the
# distribution name from the package name.
# https://docs.python.org/3/library/importlib.metadata.html#package-distributions
__version__ = importlib.metadata.version('deephaven-server')

def _start(args):
    """
    Start the Deephaven server with the given arguments.

    Args:
        args: The arguments parsed using argparse to use to start the server.
    """
    if args.key is None:
        args.key = secrets.token_urlsafe(32)

    jvm_args = [f"-Dauthentication.psk={args.key}"]
    if args.jvm_args is not None:
        jvm_args += [args.jvm_args]

    s = Server(host=args.host, port=args.port, jvm_args=jvm_args, dh_args=args.dh_args)
    s.start()

    url = f"http://{args.host}:{args.port}/ide?psk={args.key}"
    webbrowser.open(url)

    def print_info():
        print(f"Deephaven is running at {url}")
        print("Press Control-C to exit")

    print_info()

    def signal_handler(sig, frame):
        print("Exiting Deephaven...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.pause()

def main():
    """
    Main entry point for the Deephaven embedded server application.
    Accepts a command to start the application and the arguments to use.
    Defaults to `start`, which starts the server.
    """
    parser = argparse.ArgumentParser(description="Start the Deephaven embedded server")
    parser.add_argument("cmd", nargs="?", default="start", choices=["start"], help="Deephaven command to run")
    parser.add_argument("-p", "--port", default=8080, type=int, help="The port to bind to")
    parser.add_argument("--host", default="localhost", help="The host to bind to")
    parser.add_argument("--key", default=None, help="The key to use. Will randomly generate one if not specified.")
    parser.add_argument("--jvm-args", default=None, help="The JVM arguments to use")
    parser.add_argument("--dh-args", default=None, help="The Deephaven arguments to use")
    args = parser.parse_args()

    if args.cmd == "start":
        _start(args)
    else:
        parser.print_help()
        sys.exit(1)
