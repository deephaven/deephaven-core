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

// forward declaration of classes
class ClientOptionsWrapper;
class ClientWrapper;

// ######################### DH WRAPPERS #########################

class TableHandleWrapper {
public:
    TableHandleWrapper(deephaven::client::TableHandle ref_table) : internal_tbl_hdl(std::move(ref_table)) {};

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
        ArrowArrayStream* stream_ptr = new ArrowArrayStream();
        arrow::ExportRecordBatchReader(record_batch_reader, stream_ptr);

        // XPtr is needed here to ensure Rcpp can properly handle type casting, as it does not like raw pointers
        return Rcpp::XPtr<ArrowArrayStream>(stream_ptr, true);
    };

private:
    deephaven::client::TableHandle internal_tbl_hdl;
};



class ClientOptionsWrapper {
public:

    ClientOptionsWrapper() {
        internal_options = new deephaven::client::ClientOptions();
        bool authentication_set = false;
        bool session_set = false;
    };

    void setDefaultAuthentication() {
        internal_options->setDefaultAuthentication();
        authentication_set = true;
    };

    void setBasicAuthentication(const std::string &username, const std::string &password) {
        internal_options->setBasicAuthentication(username, password);
        authentication_set = true;
    };

    void setCustomAuthentication(const std::string &authenticationKey, const std::string &authenticationValue) {
        internal_options->setCustomAuthentication(authenticationKey, authenticationValue);
        authentication_set = true;
    };
    
    void setSessionType(const std::string &sessionType) {
        internal_options->setSessionType(sessionType);
        session_set = true;
    };

    bool authentication_set;
    bool session_set;

private:

    deephaven::client::ClientOptions* internal_options;
    friend ClientWrapper* newClientWrapper(const std::string &target, const ClientOptionsWrapper &client_options);
};



class ClientWrapper {
public:

    /**
     * Fetches a reference to a table named tableName on the server if it exists.
     * @param tableName Name of the table to search for.
     * @return TableHandle reference to the fetched table. 
    */
    TableHandleWrapper* openTable(std::string tableName) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.fetchTable(tableName));
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
    TableHandleWrapper* newTableFromArrowArrayStreamPtr(Rcpp::XPtr<ArrowArrayStream> stream_ptr) {

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

        return new TableHandleWrapper(new_tbl_hdl);
    };

private:
    ClientWrapper(deephaven::client::Client ref) : internal_client(std::move(ref)) {};

    const deephaven::client::Client internal_client;
    const deephaven::client::TableHandleManager internal_tbl_hdl_mngr = internal_client.getManager();

    friend ClientWrapper* newClientWrapper(const std::string &target, const ClientOptionsWrapper &client_options);
};

// factory method for calling private constructor, Rcpp does not like <const std::string &target> in constructor
// the current implementation of passing authentication args to C++ client is terrible and needs to be redone. Only this could make Rcpp happy in a days work

/**
 * Factory method for creating a new ClientWrapper, which is responsible for maintaining a connection to the client.
 * @param target URL that the server is running on.
 * @param client_options A ClientOptionsWrapper containing the server connection information. See deephaven::client::ClientOptions for more information.
 */
ClientWrapper* newClientWrapper(const std::string &target, const ClientOptionsWrapper &client_options) {
    return new ClientWrapper(deephaven::client::Client::connect(target, *client_options.internal_options));
};



// ######################### RCPP GLUE #########################

using namespace Rcpp;

RCPP_EXPOSED_CLASS(ClientOptionsWrapper)
RCPP_EXPOSED_CLASS(TableHandleWrapper)
RCPP_EXPOSED_CLASS(ArrowArrayStream)

RCPP_MODULE(DeephavenInternalModule) {

    class_<TableHandleWrapper>("INTERNAL_TableHandle")
    .method("bind_to_variable", &TableHandleWrapper::bindToVariable)
    .method("get_arrow_array_stream_ptr", &TableHandleWrapper::getArrowArrayStreamPtr)
    ;

    class_<ClientOptionsWrapper>("INTERNAL_ClientOptions")
    .constructor()
    .method("set_default_authentication", &ClientOptionsWrapper::setDefaultAuthentication)
    .method("set_basic_authentication", &ClientOptionsWrapper::setBasicAuthentication)
    .method("set_custom_authentication", &ClientOptionsWrapper::setCustomAuthentication)
    .method("set_session_type", &ClientOptionsWrapper::setSessionType)
    ;

    class_<ClientWrapper>("INTERNAL_Client")
    .factory<const std::string&, const ClientOptionsWrapper&>(newClientWrapper)
    .method("open_table", &ClientWrapper::openTable)
    .method("check_for_table", &ClientWrapper::checkForTable)
    .method("run_script", &ClientWrapper::runScript)
    .method("new_arrow_array_stream_ptr", &ClientWrapper::newArrowArrayStreamPtr)
    .method("new_table_from_arrow_array_stream_ptr", &ClientWrapper::newTableFromArrowArrayStreamPtr)
    ;
}
