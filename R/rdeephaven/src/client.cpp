#include <iostream>
#include <utility>
#include <memory>
#include <string>
#include <vector>

#include "deephaven/client/client.h"
#include "deephaven/client/flight.h"
#include "deephaven/client/utility/arrow_util.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

#include <Rcpp.h>

// forward declaration of classes
class ClientOptionsWrapper;
class ClientWrapper;

// ######################### DH WRAPPERS #########################

class TableHandleWrapper {
public:
    TableHandleWrapper(deephaven::client::TableHandle ref_table) :
        internal_tbl_hdl(std::move(ref_table)) {};

    // TODO: DEEPHAVEN QUERY METHODS WILL GO HERE

    /**
     * Whether the table was static at the time internal_tbl_hdl was created.
    */
    bool isStatic() {
        return internal_tbl_hdl.isStatic();
    }

    /**
     * Number of rows in the table at the time internal_tbl_hdl was created.
    */
    int64_t numRows() {
        return internal_tbl_hdl.numRows();
    }

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
        deephaven::client::utility::okOrThrow(DEEPHAVEN_EXPR_MSG(fsr->ReadAll(&empty_record_batches)));

        std::shared_ptr<arrow::RecordBatchReader> record_batch_reader = arrow::RecordBatchReader::Make(empty_record_batches).ValueOrDie();
        ArrowArrayStream* stream_ptr = new ArrowArrayStream();
        arrow::ExportRecordBatchReader(record_batch_reader, stream_ptr);

        // XPtr is needed here to ensure Rcpp can properly handle type casting, as it does not like raw pointers
        return Rcpp::XPtr<ArrowArrayStream>(stream_ptr, true);
    }

private:
    deephaven::client::TableHandle internal_tbl_hdl;
};


// TODO: Document this guy
class ClientOptionsWrapper {
public:

    ClientOptionsWrapper() :
        internal_options(std::make_shared<deephaven::client::ClientOptions>()) {}

    void setDefaultAuthentication() {
        internal_options->setDefaultAuthentication();
    }

    void setBasicAuthentication(const std::string &username, const std::string &password) {
        internal_options->setBasicAuthentication(username, password);
    }

    void setCustomAuthentication(const std::string &authenticationKey, const std::string &authenticationValue) {
        internal_options->setCustomAuthentication(authenticationKey, authenticationValue);
    }

    void setSessionType(const std::string &sessionType) {
        internal_options->setSessionType(sessionType);
    }

    void setUseTls(bool useTls) {
        internal_options->setUseTls(useTls);
    }

    void setTlsRootCerts(std::string tlsRootCerts) {
        internal_options->setTlsRootCerts(tlsRootCerts);
    }

    void addIntOption(std::string opt, int val) {
        internal_options->addIntOption(opt, val);
    }

    void addStringOption(std::string opt, std::string val) {
        internal_options->addStringOption(opt, val);
    }

    void addExtraHeader(std::string header_name, std::string header_value) {
        internal_options->addExtraHeader(header_name, header_value);
    }

private:
    std::shared_ptr<deephaven::client::ClientOptions> internal_options;
    friend ClientWrapper;
};



class ClientWrapper {
public:

    ClientWrapper(std::string target, const ClientOptionsWrapper &client_options) :
        internal_client(deephaven::client::Client::connect(target, *client_options.internal_options)) {}

    /**
     * Fetches a reference to a table named tableName on the server if it exists.
     * @param tableName Name of the table to search for.
     * @return TableHandle reference to the fetched table.
    */
    TableHandleWrapper* openTable(std::string tableName) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.fetchTable(tableName));
    }

    /**
     * Runs a script on the server in the console language if a console was created.
     * @param code String of the code to be executed on the server.
    */
    void runScript(std::string code) {
        internal_tbl_hdl_mngr.runScript(code);
    }

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
    }

    /**
     * Allocates memory for an ArrowArrayStream C struct and returns a pointer to the new chunk of memory.
     * Intended to be used to get a pointer to pass to Arrow's R library RecordBatchReader$export_to_c(ptr).
    */
    SEXP newArrowArrayStreamPtr() {
        ArrowArrayStream* stream_ptr = new ArrowArrayStream();
        return Rcpp::XPtr<ArrowArrayStream>(stream_ptr, true);
    }

    /**
     * Uses a pointer to a populated ArrowArrayStream C struct to create a new table on the server from the data in the C struct.
     * @param stream_ptr Pointer to an existing and populated ArrayArrayStream, populated by a call to RecordBatchReader$export_to_c(ptr) from R.
    */
    TableHandleWrapper* newTableFromArrowArrayStreamPtr(Rcpp::XPtr<ArrowArrayStream> stream_ptr) {

        auto wrapper = internal_tbl_hdl_mngr.createFlightWrapper();
        arrow::flight::FlightCallOptions options;
        wrapper.addHeaders(&options);

        // extract RecordBatchReader from the struct pointed to by the passed stream_ptr
        std::shared_ptr<arrow::RecordBatchReader> record_batch_reader = arrow::ImportRecordBatchReader(stream_ptr.get()).ValueOrDie();
        auto schema = record_batch_reader.get()->schema();

        // write RecordBatchReader data to table on server with DoPut
        std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
        std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;

        auto ticket = internal_tbl_hdl_mngr.newTicket();
        auto fd = deephaven::client::utility::convertTicketToFlightDescriptor(ticket);
        
        deephaven::client::utility::okOrThrow(DEEPHAVEN_EXPR_MSG(wrapper.flightClient()->DoPut(options, fd, schema, &fsw, &fmr)));
        while(true) {
            std::shared_ptr<arrow::RecordBatch> this_batch;
            deephaven::client::utility::okOrThrow(DEEPHAVEN_EXPR_MSG(record_batch_reader->ReadNext(&this_batch)));
            if (this_batch == nullptr) {
                break;
            }
            deephaven::client::utility::okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->WriteRecordBatch(*this_batch)));
        }
        deephaven::client::utility::okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->DoneWriting()));
        deephaven::client::utility::okOrThrow(DEEPHAVEN_EXPR_MSG(fsw->Close()));

        auto new_tbl_hdl = internal_tbl_hdl_mngr.makeTableHandleFromTicket(ticket);
        return new TableHandleWrapper(new_tbl_hdl);
    }

private:
    const deephaven::client::Client internal_client;
    const deephaven::client::TableHandleManager internal_tbl_hdl_mngr = internal_client.getManager();
};


// ######################### RCPP GLUE #########################

using namespace Rcpp;

RCPP_EXPOSED_CLASS(ClientOptionsWrapper)
RCPP_EXPOSED_CLASS(TableHandleWrapper)
RCPP_EXPOSED_CLASS(ArrowArrayStream)

RCPP_MODULE(DeephavenInternalModule) {

    class_<TableHandleWrapper>("INTERNAL_TableHandle")
    .method("is_static", &TableHandleWrapper::isStatic)
    .method("num_rows", &TableHandleWrapper::numRows)
    .method("bind_to_variable", &TableHandleWrapper::bindToVariable)
    .method("get_arrow_array_stream_ptr", &TableHandleWrapper::getArrowArrayStreamPtr)
    ;

    class_<ClientOptionsWrapper>("INTERNAL_ClientOptions")
    .constructor()
    .method("set_default_authentication", &ClientOptionsWrapper::setDefaultAuthentication)
    .method("set_basic_authentication", &ClientOptionsWrapper::setBasicAuthentication)
    .method("set_custom_authentication", &ClientOptionsWrapper::setCustomAuthentication)
    .method("set_session_type", &ClientOptionsWrapper::setSessionType)
    .method("set_use_tls", &ClientOptionsWrapper::setUseTls)
    .method("set_tls_root_certs", &ClientOptionsWrapper::setTlsRootCerts)
    .method("add_int_option", &ClientOptionsWrapper::addIntOption)
    .method("add_string_option", &ClientOptionsWrapper::addStringOption)
    .method("add_extra_header", &ClientOptionsWrapper::addExtraHeader)
    ;

    class_<ClientWrapper>("INTERNAL_Client")
    .constructor<std::string, const ClientOptionsWrapper&>()
    .method("open_table", &ClientWrapper::openTable)
    .method("check_for_table", &ClientWrapper::checkForTable)
    .method("run_script", &ClientWrapper::runScript)
    .method("new_arrow_array_stream_ptr", &ClientWrapper::newArrowArrayStreamPtr)
    .method("new_table_from_arrow_array_stream_ptr", &ClientWrapper::newTableFromArrowArrayStreamPtr)
    ;
}
