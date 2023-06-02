#include <iostream>
#include <utility>
#include <memory>
#include <string>
#include <vector>

#include "deephaven/client/client.h"
#include "deephaven/client/flight.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

#include <Rcpp.h>


// TODO: good error handling


// ######################### DH WRAPPERS, API #########################

class NewTableHandleWrapper {
public:
    NewTableHandleWrapper(deephaven::client::TableHandle ref_table) : internal_tbl_hdl(std::move(ref_table)) {};

    // DEEPHAVEN QUERY METHODS WILL GO HERE

    /**
     * Binds the table referenced by this table handle to a variable on the server called tableName.
     * Without this call, new tables are not accessible from the client.
     * @param tableName Name for the new table on the server.
    */
    void bindToVariable(std::string tableName) {
        internal_tbl_hdl.bindToVariable(tableName);
    }

    /**
     * Creates and returns a pointer to an ArrowArrayStream C struct containing the data from the table referenced by internal_tbl_hdl.
     * Intended to be used for creating an Arrow RecordBatchReader in R via RecordBatchReader$import_from_c(ptr).
    */
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

private:
    deephaven::client::TableHandle internal_tbl_hdl;
};


class NewClientWrapper {
public:

    /**
     * Fetches a reference to a table named tableName on the server if it exists.
     * @param tableName Name of the table to search for.
     * @return TableHandle reference to the fetched table. 
    */
    NewTableHandleWrapper* openTable(std::string tableName) {
        return new NewTableHandleWrapper(internal_tbl_hdl_mngr.fetchTable(tableName));
    };

    /**
     * Runs a script on the server in the console language if a console was created.
     * @param code String of the code to be executed on the server.
    */
    void runScript(std::string code) {
        internal_tbl_hdl_mngr.runScript(code);
    };

    /**
     * Checks for the existence of a table named tableName on the server.
     * @param tableName Name of the table to search for.
     * @return Boolean indicating whether tableName exists on the server or not.
    */
    bool checkForTable(std::string tableName) {
        // we have to first fetchTable to check existence, fetchTable does not fail on its own, but .observe() will fail if table doesn't exist
        deephaven::client::TableHandle table_handle = internal_tbl_hdl_mngr.fetchTable(tableName);
        try {
            table_handle.observe();
        } catch(...) {
            return false;
        }
        return true;
    };

    /**
     * Allocates memory for an ArrowArrayStream C struct and returns a pointer to the new chunk of memory.
     * Intended to be used to get a pointer to pass to Arrow's R library RecordBatchReader$export_to_c(ptr).
    */
    SEXP newArrowArrayStreamPtr() {
        ArrowArrayStream* stream_ptr = new ArrowArrayStream();
        return Rcpp::XPtr<ArrowArrayStream>(stream_ptr, true);
    };

    /**
     * Uses a pointer to a populated ArrowArrayStream C struct to create a new table on the server from the data in the C struct.
     * @param stream_ptr Pointer to an existing and populated ArrayArrayStream, populated by a call to RecordBatchReader$export_to_c(ptr) from R.
    */
    NewTableHandleWrapper* newTableFromArrowArrayStreamPtr(Rcpp::XPtr<ArrowArrayStream> stream_ptr) {

        auto wrapper = internal_tbl_hdl_mngr.createFlightWrapper();
        arrow::flight::FlightCallOptions options;
        wrapper.addAuthHeaders(&options);

        // extract RecordBatchReader from the struct pointed to by the passed tream_ptr
        std::shared_ptr<arrow::RecordBatchReader> record_batch_reader = arrow::ImportRecordBatchReader(stream_ptr.get()).ValueOrDie();
        auto schema = record_batch_reader.get()->schema();

        // write RecordBatchReader data to table on server with DoPut
        std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
        std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;
        auto [new_tbl_hdl, fd] = internal_tbl_hdl_mngr.newTableHandleAndFlightDescriptor();
        DEEPHAVEN_EXPR_MSG(wrapper.flightClient()->DoPut(options, fd, schema, &fsw, &fmr)); // TODO: need to add okOrThrow
        while(true) {
            std::shared_ptr<arrow::RecordBatch> this_batch;
            DEEPHAVEN_EXPR_MSG(record_batch_reader->ReadNext(&this_batch)); // TODO: need to add ok or throw
            if (this_batch == nullptr) {
                break;
            }
            DEEPHAVEN_EXPR_MSG(fsw->WriteRecordBatch(*this_batch)); // TODO: need to add okOrThrow
        }
        DEEPHAVEN_EXPR_MSG(fsw->DoneWriting()); // TODO: need to add okOrThrow
        DEEPHAVEN_EXPR_MSG(fsw->Close()); // TODO: need to add okOrThrow

        return new NewTableHandleWrapper(new_tbl_hdl);
    };

private:
    NewClientWrapper(deephaven::client::Client ref, const std::string &sessionType) : session_type(sessionType),
                                                                                   internal_client(std::move(ref)) {};

    const std::string session_type;
    const deephaven::client::Client internal_client;
    const deephaven::client::TableHandleManager internal_tbl_hdl_mngr = internal_client.getManager();

    friend NewClientWrapper* newNewClientWrapper(const std::string &target, const std::string &sessionType,
                                           const std::string &authType, const std::string &key, const std::string &value);
};

// factory method for calling private constructor, Rcpp does not like <const std::string &target> in constructor
// the current implementation of passing authentication args to C++ client is terrible and needs to be redone. Only this could make Rcpp happy in a days work

/**
 * Factory method for creating a new ClientWrapper, which is responsible for maintaining a connection to the client.
 * @param target URL that the server is running on.
 * @param sessionType Type of console to start with this client connection, can be "none", "python", or "groovy". The ClientWrapper::runScript() method will only work
 *                    with the language specified here.
 * @param authType Type of authentication to use, can be "default", "basic", or "custom". "basic" uses username/password auth, "custom" uses general key/value auth.
 * @param key Key credential for authentication, can be a username if authType is "basic", or a general key if authType is "custom". Set to "" if authType is "default".
 * @param value Value credential for authentication, can be a password if authType is "basic", or a general value if authType is "custom". Set to "" if authType is "default".
 */
NewClientWrapper* newNewClientWrapper(const std::string &target, const std::string &sessionType,
                                const std::string &authType, const std::string &key, const std::string &value) {

    deephaven::client::ClientOptions client_options = deephaven::client::ClientOptions();

    if (authType == "default") {
        client_options.setDefaultAuthentication();
    } else if (authType == "basic") {
        client_options.setBasicAuthentication(key, value);
    } else if (authType == "custom") {
        client_options.setCustomAuthentication(key, value);
    } else {
        client_options.setDefaultAuthentication();
    }

    if (sessionType == "python" || sessionType == "groovy") {
        client_options.setSessionType(sessionType);
    }

    return new NewClientWrapper(deephaven::client::Client::connect(target, client_options), sessionType);
};


// ######################### RCPP GLUE #########################

using namespace Rcpp;

RCPP_EXPOSED_CLASS(NewTableHandleWrapper)
RCPP_EXPOSED_CLASS(ArrowArrayStream)

RCPP_MODULE(NewClientModule) {

    class_<NewTableHandleWrapper>("INTERNAL_TableHandle")
    .method("bind_to_variable", &NewTableHandleWrapper::bindToVariable)
    .method("get_arrowArrayStream_ptr", &NewTableHandleWrapper::getArrowArrayStreamPtr)
    ;

    class_<NewClientWrapper>("INTERNAL_Client")
    .factory<const std::string&, const std::string&, const std::string&, const std::string&, const std::string&>(newNewClientWrapper)
    .method("open_table", &NewClientWrapper::openTable)
    .method("check_for_table", &NewClientWrapper::checkForTable)
    .method("run_script", &NewClientWrapper::runScript)
    .method("new_arrowArrayStream_ptr", &NewClientWrapper::newArrowArrayStreamPtr)
    .method("new_table_from_arrowArrayStream_ptr", &NewClientWrapper::newTableFromArrowArrayStreamPtr)
    ;
}
