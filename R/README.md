# Deephaven R Client

`rdeephaven` is an R package for talking to a Deephaven Core server from R. It lets you connect to a
server, create and query tables there, pull table data into an R data frame, dplyr tibble, Arrow
Table, or Arrow RecordBatchReader, push any of those back up to the server, and run Python or Groovy
scripts in the server console.

The package is a binding over the [Deephaven C++ client](../cpp-client/README.md): R6 classes in
`rdeephaven/R/` wrap [Rcpp](https://github.com/RcppCore/Rcpp) module classes defined in
`rdeephaven/src/client.cpp`, which in turn wrap the C++ client's API. Table data crosses between R
and C++ through the Arrow C stream interface, with no copies. **Building the C++ client first is a
hard prerequisite.**

The package source lives one directory down, in `R/rdeephaven/`.

Note that the R client does not support ticking subscriptions: you can create and query ticking
tables on the server, but R only ever sees point-in-time snapshots of them. See
[`DESIGN.md`](DESIGN.md) §11 for the full list of what is and is not exposed.

## Documentation

| Document | What it is |
|----------|------------|
| [`rdeephaven/BUILDING.md`](rdeephaven/BUILDING.md) | How to install the prerequisites and build the package, plus common errors, debugging with Valgrind/gdb, and code styling. **Start here.** |
| [`DESIGN.md`](DESIGN.md) | Design and implementation of the package — the R/Rcpp/C++ layering, the Arrow data path, code layout, conventions, and per-file summaries. Written for engineers and coding agents who need to *change* the code. |
| [`rdeephaven/vignettes/`](rdeephaven/vignettes) | User tutorials: `rdeephaven` (overview), `agg_by` (aggregations), `update_by` (moving/windowed operations). Run `vignette("rdeephaven")` after installing. |
| [`rdeephaven/man/`](rdeephaven/man) | Generated `.Rd` reference pages (from the roxygen comments in `rdeephaven/R/`), published at <https://deephaven.io/core/client-api/r/>. |
| [`../cpp-client/README.md`](../cpp-client/README.md) | The C++ client this package is built on, and an index of its documentation. |
| Repo root [`AGENTS.md`](../AGENTS.md) | The Java engine and server that this client talks to. |

## Quick start

Build the C++ client, then the R package, per
[`rdeephaven/BUILDING.md`](rdeephaven/BUILDING.md). Then, with a Deephaven server running:

```r
library(rdeephaven)

client <- Client$new("localhost:10000", auth_type = "psk", auth_token = "my_secret_token")

# push a local data frame to the server and get a handle to it
th <- client$import_table(data.frame(col1 = c(1, 2, 3), col2 = c("a", "b", "c")))

# run a server-side query and pull the result back
as.data.frame(th$update("col3 = col1 * 2"))

client$close()
```

Note that `library(rdeephaven)` requires `LD_LIBRARY_PATH` to include the C++ client's `lib`
directory; `source /path/to/dhcpp/env.sh` before starting R. See
[`rdeephaven/BUILDING.md`](rdeephaven/BUILDING.md) for ways to make that permanent.
