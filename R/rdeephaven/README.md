
# The Deephaven Core R Client

The Deephaven Core R client is an R package that enables R users to interface with a Deephaven server and perform various
server-side operations from the comfort of RStudio or any other R interface.

## What can the R client do?

The R Client provides the following functionalities:

1. Connect to a Deephaven server
   -   with anonymous authentication (no username or password)
   -   with basic authentication (username and password)
   -   with pre-shared key authentication (requires only a key)
   -   with custom authentication (general key-value credentials)

2. Run scripts on the server
   -   If the server is equipped with a console, run a script in that console
   -   Currently, Python and Groovy are supported

3. Utilize Deephaven's vast table API from R
   -   Create static and ticking tables on the server
   -   Construct and execute complex queries
   -   Retrieve references to tables on the server
   -   Pull table data into an [Arrow RecordBatchReader](https://arrow.apache.org/docs/r/reference/RecordBatchReader.html),
an [Arrow Table](https://arrow.apache.org/docs/6.0/r/reference/Table.html),
a [dplyr Tibble](https://tibble.tidyverse.org),
or an [R Data Frame](https://stat.ethz.ch/R-manual/R-devel/library/base/html/data.frame.html)
   -   Create new tables on the server from an [Arrow RecordBatchReader](https://arrow.apache.org/docs/r/reference/RecordBatchReader.html),
an [Arrow Table](https://arrow.apache.org/docs/6.0/r/reference/Table.html),
a [dplyr Tibble](https://tibble.tidyverse.org),
or an [R Data Frame](https://stat.ethz.ch/R-manual/R-devel/library/base/html/data.frame.html)
   -   Call Deephaven table methods with familiar R functions

## Installation

Currently, the R client is only supported on Ubuntu 20.04 or 22.04 and must be built from source.

0. We need a working installation of R on the machine where the R client will be built,
   plus the necessary dependencies for building and creating vignettes.
   The R client requires R 4.1.2 or newer; you can install R from the standard packages
   made available by Ubuntu 22.04.  If you want a newer R version or if you are running in
   Ubuntu 20.04, you should install R from CRAN:

   ```
   # Download the key and install it
   $ wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | \
       sudo gpg --dearmor -o /usr/share/keyrings/r-project.gpg

   # Add the R source list to apt's sources list
   $ echo "deb [signed-by=/usr/share/keyrings/r-project.gpg] https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/" | \
       sudo tee -a /etc/apt/sources.list.d/r-project.list

   # update the apt package list
   $ apt -y update

   # install R
   $ sudo apt -y install r-base r-recommended
   ```

   Independently of R itself, install the OS packages required for building vignettes: `libxml2-dev` and `pandoc`
   ```
   # required during the build for vignettes
   $ sudo apt -y install libxml2-dev pandoc 
   ```

1. Build the Deephaven C++ client (and dependencies) according to the instructions in the
   [Deephaven C++ client installation guide](https://github.com/deephaven/deephaven-core/blob/main/cpp-client/README.md).
   Follow the instructions at least through "Build and Install Deephaven C++ client". After that, you will have both the
   Deephaven C++ client and any C++ libraries it depends on installed in a particular directory of your choosing.
   In what follows, we assume that directory is `/path/to/dhcpp`.

3. Set environment variables from the C++ client installation required for building the R client:
   ```bash
   source /path/to/dhcpp/env.sh
   ```
   where `/path/to/dhcpp` is the directory you created in step 1 above.
   Ensure the necessary environment variables are set by checking their values as follows:
   ```
   echo $DHCPP
   echo $LD_LIBRARY_PATH
   echo $NCPUS
   echo $CMAKE_PREFIX_PATH
   ```

   For faster compilation of the R client and its dependencies (particularly the Arrow R client),
   use the following command:
   ```bash
    export MAKE="make -j$NCPUS"
   ```
   
   Refer to the instructions on the C++ client installation for more details on the `dhcpp` directory.

4. The C++ client installation will have left you with a local clone of the full [Deephaven Core Git repository](https://github.com/deephaven/deephaven-core).
   Navigate to the `deephaven-core/R/rdeephaven` directory within that clone.

   If you prefer to have an isolated directory where the Deephaven R client source code will live, use git's sparse-checkout:
   ```bash
   mkdir rdeephaven
   cd rdeephaven
   git init
   git remote add -f origin https://github.com/deephaven/deephaven-core.git
   git config core.sparseCheckout true
   echo "R/rdeephaven" >> .git/info/sparse-checkout
   git pull origin main
   ```

5. Start an R console inside the `rdeephaven` directory by running `R`. In that console, install the Deephaven R client dependencies:
   ```r
   install.packages(c('Rcpp', 'arrow', 'R6', 'dplyr', 'xml2', 'rmarkdown', 'knitr'))
   ```
   Then, exit the R console with `quit()`. From the `rdeephaven` directory, build and install the R client:
   ```r
   cd .. && R CMD build rdeephaven && R CMD INSTALL --no-multiarch --with-keep.source rdeephaven_*.tar.gz && rm rdeephaven_*.tar.gz
   ```
   This is needed over the typical `install.packages()` to ensure that the vignettes get built and installed.

6. Now, run
   ```r
   library(rdeephaven)
   ```
   in the R session, and start using the client!
   
   For an introduction to the package, run `vignette("rdeephaven")`.

7. The `LD_LIBRARY_PATH` environment variable set in step 2 is necessary for loading the R client once it is installed.
   Therefore, you must set `LD_LIBRARY_PATH` manually at the start of each session by running the following command:
   ```bash
   source /path/to/dhcpp/env.sh
   ```
   This will set `LD_LIBRARY_PATH`, as well as the other environment variables from step 2.
   
   If you would rather not have to run this command every time you open a new session, you can add the following to `$HOME/.profile`:
   ```bash
   # source DHCPP env variables if dhcpp directory exists
   current_dhcpp_dir=/path/to/dhcpp"
   if [ -d "$current_dhcpp_dir" ] ; then
       source "$current_dhcpp_dir"/env.sh
   fi
   ```
   This will automatically set `LD_LIBRARY_PATH` at the beginning of every session _for this user_, so that any process (R or RStudio) may access its value.
   If you move the dhcpp directory, or need to point R to another version of it, `current_dhcpp_dir` should be modified accordingly.

## Common Errors

- **Cannot compile the R client**

   If an error like this occurs in step 4:
   ```bash
   client.cpp:7:10: fatal error: deephaven/client/client.h: No such file or directory
    7 | #include "deephaven/client/client.h"
      |          ^~~~~~~~~~~~~~~~~~~~~~~~~~~
   compilation terminated.
   ```
   this means that the C++ compiler does not know where to find the relevant header files for the Deephaven C++ client. This can happen for a handul of reasons:
   1. Step 1 was skipped, and the Deephaven C++ client was not installed. In this case, please ensure that the client is installed before attempting to build the R client.
   2. The Deephaven C++ client is installed, but the `DHCPP` environment variable is not set. To test this, run
      ```bash
      echo $DHCPP
      ```
      If this returns an empty string, set `DHCPP` according to the instructions in step 2a with
      ```bash
      source /path/to/dhcpp/env.sh
      ```
   3. The Deephaven C++ client is installed and the `DHCPP` environment variable is set, but the current project is not configured to allow the compiler
      to access the Deephaven `dhcpp` and `src` directories. This is more difficult to give advice on, as it is an IDE-dependent problem. Consult your IDE's
      documentation on C/C++ compiler include paths for more information.

  - **Cannot load the R client**

      Once the R client is successfully installed, you may try to load it at a later date, only to find this error:
     ```bash
      > library(rdeephaven)
      Error: package or namespace load failed for ‘rdeephaven’ in dyn.load(file, DLLpath = DLLpath, ...):
       unable to load shared object '/home/user/R/x86_64-pc-linux-gnu-library/4.4/rdeephaven/libs/rdeephaven.so':
        libdhclient.so: cannot open shared object file: No such file or directory
      ```
      This very likely means that the `LD_LIBRARY_PATH` environment variable is not set. To rectify this, run
      ```bash
      source /path/to/dhcpp/env.sh
      ```
      from the parent session and try again. Alternatively, see step 6 for a way to solve this problem semi-permanently.

      RStudio presents its own solution to this problem that RStudio users may want to use instead of the semi-permanent solution in step 6.
      RStudio supports using a `.Renviron` file for setting environment variables. If the correct environment variables are currently set (see step 2),
      you can generate the right content for a `.Renviron` file by running the script at `etc/generate-dotRenviron-lines.sh`. Then, copy the output
      of that script into a new file called `.Renviron`, and save it in the `deephaven-core` directory. Then, create a new R project from the existing
      `deephaven-core` directory using RStudio, and the corresponding R session will inherit all the necessary environment variables for successful compilation.

      If RStudio Server is being used, the `.Renviron` files _must_ be set for successful compilation. _In addition_, run the `etc/generate-rserverdotconf-lines.sh` script,
      and the script's outputs to the `rserver.conf` file for the RStudio Server installation (the location of that file may depend on your particular RStudio server
      installation, but a common location is `/etc/rstudio/rserver.conf`).

## Running the unit tests

The Deephaven R client utilizes R's `testthat` package to perform unit tests. In order to run these unit tests, install `testthat` and the other dependent packages:
```r
install.packages(c('testthat', 'lubridate', 'zoo'))
```

Then, from an R session with `rdeephaven` installed, run the unit tests:
```r
library(testthat)
test_package("rdeephaven")
```

## Debugging

Because the Deephaven R client is written in C++ and wrapped with `Rcpp`, standard R-level debugging is not sufficient for many kinds of problems associated with C++ code. For this reason, debugging the R client must be done with a C++ debugger. We recommend using [Valgrind](https://valgrind.org) to check for memory bugs, and using [gdb](https://www.sourceware.org/gdb/) for thorough backtraces and general debugging.

### Running R with Valgrind

The following was taken from [this blog post](https://kevinushey.github.io/blog/2015/04/05/debugging-with-valgrind/), which has proven very useful for getting started with Valgrind.
1. Install Valgrind with `sudo apt-get install valgrind` or `sudo yum install valgrind`
2. Run R under Valgrind with `R -d valgrind`
   
OS-dependent problems may come up in either step, and the simplest solution is to use a Linux machine or VM if one is available. Attempting these steps in a Linux Docker image may also prove difficult, and will certainly fail if the host architecture is not AMD/X86.

### Running R with gdb

[This article](https://www.maths.ed.ac.uk/~swood34/RCdebug/RCdebug.html) is a good resource for running R with gdb, and also touches on Valgrind use. There are several ways to run R with gdb, and here we only outline the text-based approach given near the bottom of the page.
1. Install gdb with `sudo apt-get install gdb` or `sudo yum install gdb`
2. Start gdb with R attached with `R -d gdb`. This will start a gdb session denoted by `(gdb)` in the console.
3. In the gdb session, start an R console with `(gdb) run`

Both Valgrind and gdb debugging is done through a console, and is not interactive from an IDE. There may be a way to make RStudio play well with Valgrind or gdb, but that is beyond the scope of these instructions.

### Enabling DEBUG level logging for gRPC and the C++ layer of the Deephaven R client

The C++ component of the Deephaven R client uses the C++ implementation of gRPC to exchange messages with a Deephaven server.
gRPC has an internal logging component that can be configured to log to stderr detail information about connection state and messages
exchanged between client and server; the Deephaven R client also uses the same logging component to show client state information.
This can be useful for debugging purposes.  To enable detailed logging, set the environment variable `GRPC_VERVOSITY=DEBUG`

## Code Styling

The Deephaven R client uses the [Tidyverse styleguide](https://style.tidyverse.org) for code formatting, and implements this style with the `styler` package. For contributions, ensure that code is properly styled according to Tidyverse standards by running the following code in your R console, where `/path/to/rdeephaven` is the path to the root directory of this package.
```
setwd("/path/to/rdeephaven")
install.packages("styler")
library(styler)
style_pkg()
```
   
## High-level design overview

The R client uses the
[Deephaven C++ client](https://github.com/deephaven/deephaven-core/tree/main/cpp-client)
as the backend for connecting to and communicating with the server. Any Deephaven-specific feature in the R client is,
at some level, an API for an equivalent feature in the C++ client. 
To make Deephaven's C++ client API available in R, an R6 class provides an R interface to 
[Rcpp](https://github.com/RcppCore/Rcpp) wrapped parts of the C++ API.  Deephaven's C++ API can create Arrow tables, and R has an [Arrow library](https://github.com/apache/arrow/tree/main/r).  Because Arrow is an in-memory data format, Arrow data can be transferred between R and C++ simply by passing a pointer between the languages and using the
[Arrow C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html).  No data copies are required.  
