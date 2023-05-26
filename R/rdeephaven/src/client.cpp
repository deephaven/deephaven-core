#include <memory>
#include <exception>
#include <iostream>
#include <typeinfo>
#include <cstdint>
#include <sstream>
#include <string>

#include "deephaven/client/client.h"
#include "deephaven/client/flight.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

#include <Rcpp.h>


// **********************

#include <arrow/array.h>
#include <arrow/scalar.h>
using deephaven::client::Column;


// ######################### DH WRAPPERS, API #########################

class TableHandleWrapper {
public:
    TableHandleWrapper(deephaven::client::TableHandle ref_table) : internal_tbl_hdl(std::move(ref_table)),
                                                                   internal_stream(std::make_shared<ArrowArrayStream>()) {
        fillStream();
    };

    TableHandleWrapper* select(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.select(columnSpecs));
    };

    TableHandleWrapper* view(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.view(columnSpecs));
    };

    TableHandleWrapper* dropColumns(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.dropColumns(columnSpecs));
    };

    TableHandleWrapper* update(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.update(columnSpecs));
    };

    SEXP getStream() {
        Rcpp::XPtr<ArrowArrayStream> ptr(internal_stream.get(), true);
        return ptr;
    };

    void print() {
        std::cout << internal_tbl_hdl.stream(true) << '\n';
    };

    void printStream() {
        std::cout << internal_stream << "\n";
    };

private:
    void fillStream() {

        std::shared_ptr<arrow::flight::FlightStreamReader> fsr = internal_tbl_hdl.getFlightStreamReader();

        // allocate memory for vector of RecordBatches and populate with fsr
        std::vector<std::shared_ptr<arrow::RecordBatch>> empty_record_batches;
        DEEPHAVEN_EXPR_MSG(fsr->ReadAll(&empty_record_batches)); // need to add OK or throw

        // convert to RecordBatchReader
        arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> record_batch_reader = arrow::RecordBatchReader::Make(empty_record_batches);

        //export to C struct
        arrow::ExportRecordBatchReader(record_batch_reader.ValueOrDie(), internal_stream.get());
    };
    deephaven::client::TableHandle internal_tbl_hdl;
    std::shared_ptr<ArrowArrayStream> internal_stream;
};


class ClientWrapper {
public:
    TableHandleWrapper* emptyTable(int64_t size) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.emptyTable(size));
    };
    TableHandleWrapper* openTable(std::string tableName) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.fetchTable(tableName));
    };
    
private:
    ClientWrapper(deephaven::client::Client ref) : internal_client(std::move(ref)) {};
    const deephaven::client::Client internal_client;
    const deephaven::client::TableHandleManager internal_tbl_hdl_mngr = internal_client.getManager();
    friend ClientWrapper* newClientWrapper(const std::string &target);
};

// factory method for calling private constructor, Rcpp does not like <const std::string &target> in constructor
ClientWrapper* newClientWrapper(const std::string &target) {
    return new ClientWrapper(deephaven::client::Client::connect(target));
};


// ######################### RCPP GLUE #########################

using namespace Rcpp;

RCPP_EXPOSED_CLASS(TableHandleWrapper)
RCPP_EXPOSED_CLASS(BatchReader)
RCPP_EXPOSED_CLASS(ArrowArrayStream)

RCPP_MODULE(ClientModule) {

    class_<TableHandleWrapper>("TableHandle")
    .method("select", &TableHandleWrapper::select)
    .method("view", &TableHandleWrapper::view)
    .method("drop_columns", &TableHandleWrapper::dropColumns)
    .method("update", &TableHandleWrapper::update)
    .method("get_stream", &TableHandleWrapper::getStream)
    .method("print", &TableHandleWrapper::print)
    .method("print_stream", &TableHandleWrapper::printStream)
    ;

    class_<ClientWrapper>("Client")
    .factory<const std::string&>(newClientWrapper)
    .method("empty_table", &ClientWrapper::emptyTable)
    .method("open_table", &ClientWrapper::openTable)
    ;
}
