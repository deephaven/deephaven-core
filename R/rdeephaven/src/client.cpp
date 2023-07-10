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
    TableHandleWrapper(deephaven::client::TableHandle ref_table) : internal_tbl_hdl(std::move(ref_table)) {};

    // HELPER FUNCTIONS

    std::vector<deephaven::client::TableHandle> convertTableHandleWrapperVector(std::vector<TableHandleWrapper> input) {
        std::vector<deephaven::client::TableHandle> output(input.size());
        std::transform(input.begin(), input.end(),
                       output.begin(), [](const TableHandleWrapper& wrapper) { return wrapper.internal_tbl_hdl; });
        return output;
    };

    // TABLE OPERATIONS

    // FILTER OPERATIONS

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

    TableHandleWrapper* updateView(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.updateView(columnSpecs));
    };

    TableHandleWrapper* where(std::string condition) {
        return new TableHandleWrapper(internal_tbl_hdl.where(condition));
    };

    // AGGREGATION OPERATIONS

    TableHandleWrapper* by(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.by(columnSpecs));
    };

    TableHandleWrapper* minBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.minBy(columnSpecs));
    };

    TableHandleWrapper* maxBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.maxBy(columnSpecs));
    };

    TableHandleWrapper* sumBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.sumBy(columnSpecs));
    };

    TableHandleWrapper* absSumBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.absSumBy(columnSpecs));
    };

    TableHandleWrapper* varBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.varBy(columnSpecs));
    };

    TableHandleWrapper* stdBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.stdBy(columnSpecs));
    };

    TableHandleWrapper* avgBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.avgBy(columnSpecs));
    };

    TableHandleWrapper* firstBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.firstBy(columnSpecs));
    };

    TableHandleWrapper* lastBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.lastBy(columnSpecs));
    };

    TableHandleWrapper* medianBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.medianBy(columnSpecs));
    };

    TableHandleWrapper* percentileBy(double percentile, std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.percentileBy(percentile, columnSpecs));
    };

    TableHandleWrapper* countBy(std::string countByColumn, std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.countBy(countByColumn, columnSpecs));
    };

    TableHandleWrapper* wAvgBy(std::string weightColumn, std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.wAvgBy(weightColumn, columnSpecs));
    };

    TableHandleWrapper* tailBy(int64_t n, std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.tailBy(n, columnSpecs));
    };

    TableHandleWrapper* headBy(int64_t n, std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.headBy(n, columnSpecs));
    };

    // JOIN OPERATIONS

    TableHandleWrapper* crossJoin(const TableHandleWrapper &rightSide, std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) {
        return new TableHandleWrapper(internal_tbl_hdl.crossJoin(rightSide.internal_tbl_hdl, columnsToMatch, columnsToAdd));
    };

    TableHandleWrapper* naturalJoin(const TableHandleWrapper &rightSide, std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) {
        return new TableHandleWrapper(internal_tbl_hdl.naturalJoin(rightSide.internal_tbl_hdl, columnsToMatch, columnsToAdd));
    };

    TableHandleWrapper* exactJoin(const TableHandleWrapper &rightSide, std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) {
        return new TableHandleWrapper(internal_tbl_hdl.exactJoin(rightSide.internal_tbl_hdl, columnsToMatch, columnsToAdd));
    };

    // MISC OPERATIONS

    TableHandleWrapper* head(int64_t n) {
        return new TableHandleWrapper(internal_tbl_hdl.head(n));
    };

    TableHandleWrapper* tail(int64_t n) {
        return new TableHandleWrapper(internal_tbl_hdl.tail(n));
    };

    TableHandleWrapper* ungroup(bool nullFill, std::vector<std::string> groupByColumns) {
        return new TableHandleWrapper(internal_tbl_hdl.ungroup(nullFill, groupByColumns));
    };

    // TODO: TableHandleWrapper* sort(std::vector<SortPair> sortPairs) {
    //      return new TableHandleWrapper(internal_tbl_hdl.sort(sortPairs));
    //  };

    // TODO: TableHandleWrapper* merge(std::string keyColumn, std::vector<TableHandleWrapper> sources) {
    //     return new TableHandleWrapper(internal_tbl_hdl.merge(keyColumn, convertTableHandleWrapperVector(sources)));
    // };

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

    ClientOptionsWrapper() {
        internal_options = new deephaven::client::ClientOptions();
    }

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
    }

    TableHandleWrapper* emptyTable(int64_t size) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.emptyTable(size));
    }

    TableHandleWrapper* timeTable(int64_t startTimeNanos, int64_t periodNanos) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.timeTable(startTimeNanos, periodNanos));
    };

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
    .method("select", &TableHandleWrapper::select)
    .method("view", &TableHandleWrapper::view)
    .method("drop_columns", &TableHandleWrapper::dropColumns)
    .method("update", &TableHandleWrapper::update)
    .method("update_view", &TableHandleWrapper::updateView)
    .method("where", &TableHandleWrapper::where)
    .method("by", &TableHandleWrapper::by)
    .method("min_by", &TableHandleWrapper::minBy)
    .method("max_by", &TableHandleWrapper::maxBy)
    .method("sum_by", &TableHandleWrapper::sumBy)
    .method("abs_sum_by", &TableHandleWrapper::absSumBy)
    .method("var_by", &TableHandleWrapper::varBy)
    .method("std_by", &TableHandleWrapper::stdBy)
    .method("avg_by", &TableHandleWrapper::avgBy)
    .method("first_by", &TableHandleWrapper::firstBy)
    .method("last_by", &TableHandleWrapper::lastBy)
    .method("median_by", &TableHandleWrapper::medianBy)
    .method("percentile_by", &TableHandleWrapper::percentileBy)
    .method("count_by", &TableHandleWrapper::countBy)
    .method("w_avg_by", &TableHandleWrapper::wAvgBy)
    .method("tail_by", &TableHandleWrapper::tailBy)
    .method("head_by", &TableHandleWrapper::headBy)
    .method("cross_join", &TableHandleWrapper::crossJoin)
    .method("natural_join", &TableHandleWrapper::naturalJoin)
    .method("exact_join", &TableHandleWrapper::exactJoin)
    .method("head", &TableHandleWrapper::head)
    .method("tail", &TableHandleWrapper::tail)
    .method("ungroup", &TableHandleWrapper::ungroup)
    // TODO: .method("sort", &TableHandleWrapper::sort)
    // TODO: .method("merge", &TableHandleWrapper::merge)
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
    .factory<const std::string&, const ClientOptionsWrapper&>(newClientWrapper)
    .method("open_table", &ClientWrapper::openTable)
    .method("empty_table", &ClientWrapper::emptyTable)
    .method("time_table", &ClientWrapper::timeTable)
    .method("check_for_table", &ClientWrapper::checkForTable)
    .method("run_script", &ClientWrapper::runScript)
    .method("new_arrow_array_stream_ptr", &ClientWrapper::newArrowArrayStreamPtr)
    .method("new_table_from_arrow_array_stream_ptr", &ClientWrapper::newTableFromArrowArrayStreamPtr)
    ;
}
