#include "deephaven/client/client.h"
#include <Rcpp.h>

// first, we define a wrapper for the Client class
class ClientWrapper {
public:
    ClientWrapper() {
        ptr_ = std::make_shared<deephaven::client::Client>();
    };
    deephaven::client::Client connect(const std::string &target) {
        return ptr_->connect(target);
    }
private:
    std::shared_ptr<deephaven::client::Client> ptr_;
    friend <Rcpp::XPtr> ClientWrapper__new();
    friend <> ClientWrapper__connect(SEXP xp, SEXP target_);
};


// next, we wrap that in an Rcpp module to expose the wrapper we created to R
namespace Rcpp {

RcppExport SEXP ClientWrapper__new() {
    Rcpp::XPtr<ClientWrapper>
    ptr(new ClientWrapper, true);
    return ptr;
}

RcppExport SEXP ClientWrapper__connect(SEXP xp, SEXP target_) {
    Rcpp::XPtr<ClientWrapper> ptr(xp);
    const std::string &target = as<const std::string>(target_);
    ClientWrapper connect_obj = ptr->connect(target) // doesnt make sense
    return connect_obj;
}

}