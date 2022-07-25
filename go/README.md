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
All tests should pass within 30 seconds.
Tests assume the server is on `localhost:10000` by default, but can be configured by setting the `DH_HOST` and `DH_PORT` environment variables.

## Examples

This project also includes several example applications.
Examples assume the server is running on `localhost:10000` by default,
but can be configured by setting the `DH_HOST` and `DH_PORT` environment variables.
An example can be run using one of the following commands:
```bash
$ go test -v ./client/example_basic_query_test.go
$ go test -v ./client/example_fetch_table_test.go
$ go test -v ./client/example_import_table_test.go
$ go test -v ./client/example_input_table_test.go
$ go test -v ./client/example_run_script_test.go
```

The source code is available in the directory for each example.

## Viewing Docs

Online documentation for this package can be found [on this page](https://pkg.go.dev/github.com/deephaven/deephaven-core/go/).

To generate and view docs locally, use Godoc:
```
$ go get -v golang.org/x/tools/cmd/godoc  # Installation, only needs to be done once
$ godoc
```
This will start up a web server (by default on [`http://localhost:6060`](http://localhost:6060)) that can be opened in a browser.
The docs for the package will be at [this link](http://localhost:6060/pkg/github.com/deephaven/deephaven-core/go/pkg/client/).