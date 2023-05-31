#include <memory>
#include <exception>
#include <iostream>
#include <sstream>
#include <string>

#include "deephaven/client/client.h"
#include "deephaven/client/flight.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

#include <Rcpp.h>


// TODO: determine if Rcpp generates docs from here and document for internal use
// TODO: good error handling


// ######################### DH WRAPPERS, API #########################

class TableHandleWrapper {
public:
    TableHandleWrapper(deephaven::client::TableHandle ref_table) : internal_tbl_hdl(std::move(ref_table)) {};

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

    SEXP getArrowArrayStreamPtr() {

        std::shared_ptr<arrow::flight::FlightStreamReader> fsr = internal_tbl_hdl.getFlightStreamReader();

        std::vector<std::shared_ptr<arrow::RecordBatch>> empty_record_batches;
        DEEPHAVEN_EXPR_MSG(fsr->ReadAll(&empty_record_batches)); // need to add OK or throw

        std::shared_ptr<arrow::RecordBatchReader> record_batch_reader = arrow::RecordBatchReader::Make(empty_record_batches).ValueOrDie();

        // create C struct and export RecordBatchReader to that struct
        ArrowArrayStream* stream_ptr = new ArrowArrayStream();
        arrow::ExportRecordBatchReader(record_batch_reader, stream_ptr);

        // XPtr is needed here to ensure Rcpp can properly handle type casting, as it does not like raw pointers
        return Rcpp::XPtr<ArrowArrayStream>(stream_ptr, true);
    };

    void print() {
        std::cout << internal_tbl_hdl.stream(true) << '\n';
    };

private:
    deephaven::client::TableHandle internal_tbl_hdl;
};


class ClientWrapper {
public:

    TableHandleWrapper* emptyTable(int64_t size) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.emptyTable(size));
    };
    TableHandleWrapper* openTable(std::string tableName) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.fetchTable(tableName));
    };

    void runScript(std::string code) {
        internal_tbl_hdl_mngr.runScript(code);
    };

    // INTERNAL USE METHODS

    SEXP newArrowArrayStreamPtr() {

        ArrowArrayStream* stream_ptr = new ArrowArrayStream();
        return Rcpp::XPtr<ArrowArrayStream>(stream_ptr, true);
    };

    TableHandleWrapper* newTableFromArrowArrayStreamPtr(Rcpp::XPtr<ArrowArrayStream> stream_ptr, std::string new_table_name) {

        // this is all essentially preamble to enable us to write to the server
        auto wrapper = internal_tbl_hdl_mngr.createFlightWrapper();
        auto [new_tbl_hdl, fd] = internal_tbl_hdl_mngr.newTableHandleAndFlightDescriptor();
        std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
        std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
        arrow::flight::FlightCallOptions options;
        wrapper.addAuthHeaders(&options);

        // here we extract the RecordBatchReader from the struct pointed to by the passed tream_ptr
        std::shared_ptr<arrow::RecordBatchReader> record_batch_reader = arrow::ImportRecordBatchReader(stream_ptr.get()).ValueOrDie();
        auto schema = record_batch_reader.get()->schema();

        // write RecordBatchReader data to table on server with DoPut
        DEEPHAVEN_EXPR_MSG(wrapper.flightClient()->DoPut(options, fd, schema, &fsw, &fmr)); // need to add okOrThrow
        while(true) {
            std::shared_ptr<arrow::RecordBatch> this_batch;
            DEEPHAVEN_EXPR_MSG(record_batch_reader->ReadNext(&this_batch)); // need to add ok or throw
            if (this_batch == nullptr) {
                break;
            }
            DEEPHAVEN_EXPR_MSG(fsw->WriteRecordBatch(*this_batch)); // need to add okOrThrow
        }
        DEEPHAVEN_EXPR_MSG(fsw->DoneWriting()); // need to add okOrThrow
        DEEPHAVEN_EXPR_MSG(fsw->Close()); // need to add okOrThrow

        // assign name to new table and return
        new_tbl_hdl.bindToVariable(new_table_name);
        return new TableHandleWrapper(new_tbl_hdl);
    };

private:
    ClientWrapper(deephaven::client::Client ref) : internal_client(std::move(ref)) {};
    deephaven::client::Client internal_client;
    const deephaven::client::TableHandleManager internal_tbl_hdl_mngr = internal_client.getManager();

    friend ClientWrapper* newClientWrapper(const std::string &target, const std::string &authType, const std::vector<std::string> &credentials, const std::string &sessionType);
};

// factory method for calling private constructor, Rcpp does not like <const std::string &target> in constructor
// the current implementation of passing authentication args to C++ client is terrible and needs to be redone. Only this could make Rcpp happy in a days work
ClientWrapper* newClientWrapper(const std::string &target, const std::string &authType, const std::vector<std::string> &credentials, const std::string &sessionType) {

    deephaven::client::ClientOptions client_options = deephaven::client::ClientOptions();

    if (authType == "default") {
        client_options.setDefaultAuthentication();
    } else if (authType == "basic") {
        std::cout << "basic auth!\n";
        client_options.setBasicAuthentication(credentials[0], credentials[1]);
    } else if (authType == "custom") {
        std::cout << "custom auth!\n";
        client_options.setCustomAuthentication(credentials[0], credentials[1]);
    } else {
        std::cout << "complain about invalid authType\n";
    }

    if (sessionType == "none") {}
    else if (sessionType == "python" || sessionType == "groovy") {
        client_options.setSessionType(sessionType);
    } else {
        std::cout << "complain about invalid sessionType\n";
    }

    return new ClientWrapper(deephaven::client::Client::connect(target, client_options));
};


// ######################### RCPP GLUE #########################

using namespace Rcpp;

RCPP_EXPOSED_AS(arrow::RecordBatchReader)
RCPP_EXPOSED_CLASS(TableHandleWrapper)
RCPP_EXPOSED_CLASS(ArrowArrayStream)

RCPP_MODULE(ClientModule) {

    class_<TableHandleWrapper>("TableHandle")
    .method("select", &TableHandleWrapper::select)
    .method("view", &TableHandleWrapper::view)
    .method("drop_columns", &TableHandleWrapper::dropColumns)
    .method("update", &TableHandleWrapper::update)
    .method("print", &TableHandleWrapper::print)
    .method(".get_arrowArrayStream_ptr", &TableHandleWrapper::getArrowArrayStreamPtr)
    ;

    class_<ClientWrapper>("Client")
    .factory<const std::string&, const std::string&, const std::vector<std::string>&, const std::string&>(newClientWrapper)
    .method("empty_table", &ClientWrapper::emptyTable)
    .method("open_table", &ClientWrapper::openTable)
    .method("run_script", &ClientWrapper::runScript)
    .method(".new_arrowArrayStream_ptr", &ClientWrapper::newArrowArrayStreamPtr)
    .method(".new_table_from_arrowArrayStream_ptr", &ClientWrapper::newTableFromArrowArrayStreamPtr)
    ;
}
