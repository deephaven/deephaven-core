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

class TableHandleWrapper {
public:
    TableHandleWrapper(deephaven::client::TableHandle ref_table) : internal_tbl_hdl(std::move(ref_table)) {};

    /**
     * All of the following methods that return TableHandleWrapper* call directly into the C++ client.
     * Please see the corresponding methods in deephaven/client/client.h for C++ level documentation.
    */

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

    /////////////////////////////// INTERNAL METHODS TO USE FROM R

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


class ClientWrapper {
public:

    /**
     * Fetches a reference to a table named tableName on the server if it exists.
     * @param tableName Name of the table to search for.
     * @return TableHandle reference to the fetched table. 
    */
    TableHandleWrapper* openTable(std::string tableName) {
        if (!checkForTable(tableName)) {
            std::cout << "Error: The table you've requested does not exist on the server.\n";
            return NULL;
        }
        return new TableHandleWrapper(internal_tbl_hdl_mngr.fetchTable(tableName));
    };

    /**
     * Deletes a table named tableName from the server if it exists. THIS IS ONLY VALID if tableName has NO children on the sever. DELTE THEM FIRST.
     * @param tableName Name of the table to delete.
    */
    void deleteTable(std::string tableName) {
        if (!checkForTable(tableName)) {
            std::cout << "Error: The table you're trying to delete does not exist on the server.\n";
        }
        if (session_type == "python") {
            std::string delete_script = "del(" + tableName + ")";
            runScript(delete_script);
        }
        else if (session_type == "groovy") {
            std::string delete_script = tableName + " = null";
            runScript(delete_script);
        }
        else {
            std::cout << "Error: Cannot delete table without an active console.";
        }
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
     * Runs a script on the server in the console language if a console was created.
     * @param code String of the code to be executed on the server.
    */
    void runScript(std::string code) {
        if (!(session_type == "python" || session_type == "groovy")) {
            std::cout << "Error: Cannot run a script without an active console.";
        }
        internal_tbl_hdl_mngr.runScript(code);
    };

    /////////////////////////////// INTERNAL METHODS TO USE FROM R

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
     * @param new_table_name Name for the new table that will appear on the server.
    */
    TableHandleWrapper* newTableFromArrowArrayStreamPtr(Rcpp::XPtr<ArrowArrayStream> stream_ptr, std::string new_table_name) {

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

        new_tbl_hdl.bindToVariable(new_table_name);
        return new TableHandleWrapper(new_tbl_hdl);
    };

private:
    ClientWrapper(deephaven::client::Client ref, const std::string &sessionType) : session_type(sessionType),
                                                                                   internal_client(std::move(ref)) {};

    const std::string session_type;
    const deephaven::client::Client internal_client;
    const deephaven::client::TableHandleManager internal_tbl_hdl_mngr = internal_client.getManager();

    friend ClientWrapper* newClientWrapper(const std::string &target, const std::string &sessionType,
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
ClientWrapper* newClientWrapper(const std::string &target, const std::string &sessionType,
                                const std::string &authType, const std::string &key, const std::string &value) {

    deephaven::client::ClientOptions client_options = deephaven::client::ClientOptions();

    if (authType == "default") {
        client_options.setDefaultAuthentication();
    } else if (authType == "basic") {
        client_options.setBasicAuthentication(key, value);
    } else if (authType == "custom") {
        client_options.setCustomAuthentication(key, value);
    } else {
        std::cout << "complain about invalid authType\n";
    }

    if (sessionType == "none") {}
    else if (sessionType == "python" || sessionType == "groovy") {
        client_options.setSessionType(sessionType);
    } else {
        std::cout << "complain about invalid sessionType\n";
    }

    return new ClientWrapper(deephaven::client::Client::connect(target, client_options), sessionType);
};


// ######################### RCPP GLUE #########################

using namespace Rcpp;

RCPP_EXPOSED_CLASS(TableHandleWrapper)
RCPP_EXPOSED_CLASS(ArrowArrayStream)

RCPP_MODULE(ClientModule) {

    class_<TableHandleWrapper>("TableHandle")
    .method("select", &TableHandleWrapper::select)
    .method("view", &TableHandleWrapper::view)
    .method("drop_columns", &TableHandleWrapper::dropColumns)
    .method("update", &TableHandleWrapper::update)
    .method(".get_arrowArrayStream_ptr", &TableHandleWrapper::getArrowArrayStreamPtr)
    ;

    class_<ClientWrapper>("Client")
    .factory<const std::string&, const std::string&, const std::string&, const std::string&, const std::string&>(newClientWrapper)
    .method("open_table", &ClientWrapper::openTable)
    .method("delete_table", &ClientWrapper::deleteTable)
    .method("check_for_table", &ClientWrapper::checkForTable)
    .method("run_script", &ClientWrapper::runScript)
    .method(".new_arrowArrayStream_ptr", &ClientWrapper::newArrowArrayStreamPtr)
    .method(".new_table_from_arrowArrayStream_ptr", &ClientWrapper::newTableFromArrowArrayStreamPtr)
    ;
}
