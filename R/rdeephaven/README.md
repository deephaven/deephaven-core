# rdeephaven

This directory is the source of `rdeephaven`, the Deephaven Core R client package: an R interface to
a Deephaven server, built on top of the Deephaven C++ client via Rcpp.

- **To build and install it** — including the prerequisites, common errors, and debugging tips — see
  [`BUILDING.md`](BUILDING.md) in this directory.
- **For an overview of the R client and an index of its documentation**, see
  [`../README.md`](../README.md).
- **For how the package is implemented** (the R/Rcpp/C++ layering, the Arrow data path, code layout,
  conventions), see [`../DESIGN.md`](../DESIGN.md).
- **For usage tutorials**, install the package and run `vignette("rdeephaven")`,
  `vignette("agg_by")`, or `vignette("update_by")`. The generated API reference is published at
  <https://deephaven.io/core/client-api/r/>.
