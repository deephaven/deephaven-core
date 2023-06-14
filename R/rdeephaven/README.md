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
   -   If the server is equipped with a console, run a script in that console
   -   Currently, Python and Groovy are supported

## Installation

Currently, the R client is only supported on Ubuntu 20.04 or 22.04 and must be built from source, since the build process
makes heavy use of the build instructions provided [here](https://github.com/deephaven/deephaven-core/tree/main/cpp-client).

1. Clone this repository into a local directory using git's sparse-checkout or subversion.
2. From the new local copy of this repo, run the following commands:
   ```
   cd lib
   chmod +x build-cpp.sh
   ./build-cpp.sh
   ```
   This will build all of the C++ client dependencies in a manner that is compatible with Rcpp, and will then build the Deephaven C++ client.
3. With the C++ client installed, start an R console and build and install the R client as follows:
   ```
   R
   >> install.packages("path/to/rdeephaven", repo=NULL, type="source", dependencies=TRUE)
   ```
   This last command can also be executed from RStudio without the need for explicitly starting an R console.
   
   Unless there are already packages installed that were built from source and utilized Rcpp, there will likely be many dependencies that
   need to be installed in this step. This command should take care of all of those dependencies, but it does not always work perfectly.
   Any remaining dependencies will have to be tracked down and installed manually.
4. Now, run
   ```
   library(rdeephaven)
   ```
   in the R session, and start using the client!
   
## High-level design overview

The R client uses the
[Deephaven C++ client](https://github.com/deephaven/deephaven-core/tree/main/cpp-client)
as the backend for connecting to and communicating with the server. Any Deephaven-specific feature in the R client is,
at some level, an API for an equivalent feature in the C++ client. Porting Deephaven's C++ classes to R utilizes the
[Rcpp](https://github.com/RcppCore/Rcpp)
library in conjunction with a thin C++ layer that only exposes relevant parts of the C++ client, and an R6 class wrapping
on the Rcpp-generated S4 classes. If the user needs to transfer real data from the server to the R instance or vice-versa, the
[Arrow C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
is used to faciliate efficient inter-language data transfer between C++ and R. This works because Deephaven Tables are internally
built from Arrow Tables on the C++ side, and Arrow has an
[R library](https://github.com/apache/arrow/tree/main/r)
that provides tables that can speak C Stream Interface.
