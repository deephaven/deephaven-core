#include "deephaven/client/client.h"
#include <Rcpp.h>

namespace Rcpp {

RCPP_MODULE(ClientCaller) {
  class_<deephaven::client::Client>("Client")
  .constructor();
}

}