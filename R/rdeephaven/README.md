# The Deephaven R Client

The Deephaven R client is an R package that enables R users to interface with a Deephaven server and perform various
server-side operations from the comfort of RStudio or any other R interface.

## What can the R client do?

The Deephaven Client currently provides three basic functionalities:

1. Connecting to a Deephaven server
   -   with anonymous authentication, without the requirement for a username or password
   -   with basic authentication that requires a username and password
   -   with custom authentication that requires general key-value credentials

2. Interfacing with Deephaven Tables on the server
   -   Retrieve references to tables on the server without importing any of their data locally
   -   Pull table data into an [Arrow RecordBatchReader](https://arrow.apache.org/docs/r/reference/RecordBatchReader.html),
an [Arrow Table](https://arrow.apache.org/docs/r/reference/Table.html),
a [dplyr Tibble](https://tibble.tidyverse.org),
or an [R Data Frame](https://stat.ethz.ch/R-manual/R-devel/library/base/html/data.frame.html)
   -   Create new tables on the server from any of the formats listed above
   -   Bind new server-side tables to variable names, enabling access from outside the current R session

3. Running scripts on the server
   -   If your server is equipped with a console, run a script in that console
   -   Currently, Python and Groovy are supported

## Installation

Currently, the R client is only supported on Ubuntu 20.04 or 22.04 and must be built from source, since we make heavy use of the
build instructions provided [here](https://github.com/deephaven/deephaven-core/tree/main/cpp-client).

1. Clone this repository into a local directory. You can use git's sparse-checkout or subversion to clone only this subdirectory.
2. From the new local copy of this repo, run the following commands:
   ```
   cd lib
   chmod +x build-cpp.sh
   ./build-cpp.sh
   ```
   This will build all of the C++ client dependencies in a manner that is compatible with Rcpp, and will then build the Deephaven C++ client.
3. With the C++ client installed, you can now start an R console to build and install the R package. Run the following commands to do so:
   ```
   R
   install.packages("path/to/rdeephaven", repo=NULL, type="source", dependencies=TRUE)
   ```
   This will open up an R console and build and install `rdeephaven` to your machine. You can also use RStudio for this step.
   Unless you've built R packages that utilize Rcpp before, there will likely be many dependencies that need to be installed in this step.
   If you're lucky, this command will install them all for you, and it will take a while. Otherwise, you may have to manually track down all of
   the package dependencies, which can be a huge hassle.
4. Now, you can run
   ```
   library(rdeephaven)
   ```
   in your R session, and start using the client!
   
## High-level design overview

The R client uses the
[Deephaven C++ client](https://github.com/deephaven/deephaven-core/tree/main/cpp-client)
as the backend for connecting to and communicating with the server. Any Deephaven-specific feature in the R client is,
at some level, an API for an equivalent feature in the C++ client. To port Deephaven's C++ classes to R, we use the
[Rcpp](https://github.com/RcppCore/Rcpp)
library in conjunction with a thin C++ layer that only exposes relevant parts of the C++ client and an R6 class wrapping
on the Rcpp-generated S4 classes. If the user needs to transfer real data from the server to the R instance or vice-versa,
we use the
[Arrow C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
to faciliate efficient inter-language data transfer between C++ and R. We can do this because Deephaven Tables are internally
built from Arrow Tables on the C++ side, and Arrow has an
[R library](https://github.com/apache/arrow/tree/main/r)
that provides tables that can speak C Stream Interface.
