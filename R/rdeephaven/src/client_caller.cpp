#include "deephaven/client/client.h"
#include <Rcpp.h>

namespace Rcpp {

RCPP_MODULE(client_module) {

    class_<deephaven::client::Client>("Client")
    .constructor<const std::string&>();

}

}