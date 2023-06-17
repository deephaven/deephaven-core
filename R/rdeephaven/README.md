# The Deephaven R Client

The Deephaven R client is an R package that enables R users to interface with a Deephaven server and perform various
server-side operations from the comfort of RStudio or any other R interface.

## What can the R client do?

The Deephaven Client currently provides three basic functionalities:

1. Connect to a Deephaven server
   -   with anonymous authentication (no username or password)
   -   with basic authentication (username and password)
   -   with custom authentication (general key-value credentials)

2. Interface with Deephaven Tables on the server
   -   Retrieve references to tables on the server
   -   Pull table data into an [Arrow RecordBatchReader](https://arrow.apache.org/docs/r/reference/RecordBatchReader.html),
an [Arrow Table](https://arrow.apache.org/docs/r/reference/Table.html),
a [dplyr Tibble](https://tibble.tidyverse.org),
or an [R Data Frame](https://stat.ethz.ch/R-manual/R-devel/library/base/html/data.frame.html)
   -   Create new tables on the server from an [Arrow RecordBatchReader](https://arrow.apache.org/docs/r/reference/RecordBatchReader.html),
an [Arrow Table](https://arrow.apache.org/docs/r/reference/Table.html),
a [dplyr Tibble](https://tibble.tidyverse.org),
or an [R Data Frame](https://stat.ethz.ch/R-manual/R-devel/library/base/html/data.frame.html)
   -   Bind server-side tables to variable names, enabling access from outside the current R session

3. Run scripts on the server
   -   If the server is equipped with a console, run a script in that console
   -   Currently, Python and Groovy are supported

## Installation

Currently, the R client is only supported on Ubuntu 20.04 or 22.04 and must be built from source.

0. Build the cpp-client (and any dependent libraries) according to the instructions in
   https://github.com/deephaven/deephaven-core/blob/main/cpp-client/README.md.
   Follow the instructions at least to the point for "Build and install Deephaven C++ client".
   At that point you would have both the Deephaven C++ client and any C++ libraries it depends on,
   all installed in a particular directory of your choosing.
   Define an environment variable `DHCPP` and assign it an absolute path to that directory.
   The instructions that follow assume that `$DHCPP` points there.
1. Choose a directory where the Deephaven R client source code will live.
   Here, the source code will be downloaded into a new directory called `rdeephaven`.
   Navigate into that directory and clone this subdirectory of `deephaven-core` using git's sparse-checkout:
   ```bash
   mkdir rdeephaven
   cd rdeephaven
   git init
   git remote add -f origin https://github.com/deephaven/deephaven-core.git
   git config core.sparseCheckout true
   echo "R/rdeephaven" >> .git/info/sparse-checkout
   git pull origin main
   ```
2. Copy the file in `R/rdeephaven/src/Makevars.in` to `R/rdeephaven/src/Makevars` and
   in the fourth line substitute the location of the Deephaven C++ client installtion you produced
   above.  Eg, if your Deephaven C++ client installation is in `/home/cfs/dhcpp`,
   the line should read
   ```bash
   DHCPP = /home/cfs/dhcpp
   ```

3. Start an R console with this command:
   ```bash
   R
   ```
   and in that console, install the client (replace repos with your choice):
   ```r
   install.packages("/path/to/rdeephaven", repos="https://packagemanager.rstudio.com/all/__linux__/jammy/latest", type="source", dependencies=TRUE)
   ```
   This last command can also be executed from RStudio without the need for explicitly starting an R console.
5. Now, run
   ```r
   library(rdeephaven)
   ```
   in the R session, and start using the client!
   
## High-level design overview

The R client uses the
[Deephaven C++ client](https://github.com/deephaven/deephaven-core/tree/main/cpp-client)
as the backend for connecting to and communicating with the server. Any Deephaven-specific feature in the R client is,
at some level, an API for an equivalent feature in the C++ client. 
To make Deephaven's C++ client API available in R, an R6 class provides an R interface to 
[Rcpp](https://github.com/RcppCore/Rcpp) wrapped parts of the C++ API.  Deephaven's C++ API can create Arrow tables, and R has an [Arrow library](https://github.com/apache/arrow/tree/main/r).  Because Arrow is an in-memory data format, Arrow data can be transferred between R and C++ simply by passing a pointer between the languages and using the
[Arrow C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html).  No data copies are required.  
