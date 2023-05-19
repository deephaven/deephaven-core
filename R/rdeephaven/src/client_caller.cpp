#include <memory>
#include <exception>
#include <iostream>

#include "deephaven/client/client.h"

#include <Rcpp.h>

// ######################### DH WRAPPER #########################

class ClientWrapper {
public:
    // this will become private
    ClientWrapper(deephaven::client::Client ref) : internal_client(std::move(ref)) {
    };
    deephaven::client::Client internal_client;
private:
    friend std::shared_ptr<ClientWrapper> connect_to_server(const std::string &target);
};

// external method to create and connect to server via ClientWrapper
std::shared_ptr<ClientWrapper> connect_to_server(const std::string &target) {
    return std::shared_ptr<ClientWrapper>(new ClientWrapper(deephaven::client::Client::connect(target)));
}


// ######################### RCPP WRAPPER #########################

// Wrapper function to create an external pointer
Rcpp::XPtr<std::shared_ptr<ClientWrapper>> createClientWrapper(const std::string &target) {
  // Create the MyObject using createObject function
  std::shared_ptr<ClientWrapper> client_wrapper = connect_to_server(target);

  // Create an external pointer from the shared pointer
  Rcpp::XPtr<std::shared_ptr<ClientWrapper>> ptr(new std::shared_ptr<ClientWrapper>(client_wrapper));

  // Return the external pointer
  return ptr;
}

// ######################### RCPP GLUE #########################


RCPP_MODULE(ClientModule) {
    using namespace Rcpp;

    function("createClientWrapper", &createClientWrapper);
}
