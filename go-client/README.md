# Deephaven Go Client

This is a Go package that provides a client interface for [Deephaven Community Core](https://github.com/deephaven/deephaven-core).
Details on usage can be found in the examples and in the documentation (both listed below).

## Setup

First, you will need a working Go install (1.13+ is required; so apt-get is often too outdated).
It can be installed from [the official site](https://go.dev/doc/install).

In order to run tests or examples, a Deephaven server must be running. 
To run a server, follow the instructions [here](https://github.com/deephaven/deephaven-core#run-deephaven).

## Tests

To run tests, simply run the following command from this folder.
```
$ go test -v ./...
```
All tests should pass within less than 10 seconds.
Tests assume the server is on localhost:10000 by default, but can be configured by setting the DH_HOST and DH_PORT environment variables.

## Examples

This project also includes a binary containing some examples.
Examples assume the server is running on localhost:10000.
The binary can be built using `go build` (the output is `./go-client`),
but it can also be run more directly by running the following command:
```
$ go run .
```

Running the program without arguments will list the available tests, and then one can be selected like this:
```
$ go run . 
```

The source code is available in the `examples/` directory.

## Viewing Docs

Reading through the docs in source code can be inconvenient, Godoc provides a better interface.
```
$ go get -v golang.org/x/tools/cmd/godoc  # Installation, only needs to be done once
$ godoc
```

This will start up a web server (by default on localhost:6060) that can be opened in a browser.
Use Ctrl-F to search for `go-client`. Behind the link will be the docs for the entire project.