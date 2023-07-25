#include <iostream>
#include <utility>
#include <memory>
#include <string>
#include <vector>

#include "deephaven/client/client.h"
#include "deephaven/client/columns.h"
#include "deephaven/client/flight.h"
#include "deephaven/client/utility/arrow_util.h"

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

#include <Rcpp.h>

// forward declaration of classes
class AggregateWrapper;
class TableHandleWrapper;
class ClientOptionsWrapper;
class ClientWrapper;
class SortPairWrapper;

// forward declaration of conversion functions
std::vector<deephaven::client::Aggregate> convertRcppListToVectorOfTypeAggregate(Rcpp::List rcpp_list);
std::vector<deephaven::client::TableHandle> convertRcppListToVectorOfTypeTableHandle(Rcpp::List rcpp_list);
std::vector<deephaven::client::SortPair> convertRcppListToVectorOfTypeSortPair(Rcpp::List rcpp_list);


// ######################### DH WRAPPERS #########################

class AggregateWrapper {
public:
    AggregateWrapper();
    AggregateWrapper(deephaven::client::Aggregate aggregate) :
        internal_aggregation(std::move(aggregate)) {}
private:
    deephaven::client::Aggregate internal_aggregation;
    friend TableHandleWrapper;
    friend std::vector<deephaven::client::Aggregate> convertRcppListToVectorOfTypeAggregate(Rcpp::List rcpp_list);
};

// ######################### conversion function for the above class
std::vector<deephaven::client::Aggregate> convertRcppListToVectorOfTypeAggregate(Rcpp::List rcpp_list) {
    std::vector<deephaven::client::Aggregate> converted_list;
    converted_list.reserve(rcpp_list.size());

    for(int i = 0; i < rcpp_list.size(); i++) {
        Rcpp::Environment rcpp_list_element = rcpp_list[i];
        Rcpp::XPtr<AggregateWrapper> xptr(rcpp_list_element.get(".pointer"));
        deephaven::client::Aggregate internal_aggregation = xptr->internal_aggregation;
        converted_list.push_back(internal_aggregation);
    }
    return converted_list;
}

AggregateWrapper* INTERNAL_min(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::min(columnSpecs));
}

AggregateWrapper* INTERNAL_max(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::max(columnSpecs));
}

AggregateWrapper* INTERNAL_sum(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::sum(columnSpecs));
}

AggregateWrapper* INTERNAL_absSum(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::absSum(columnSpecs));
}

AggregateWrapper* INTERNAL_avg(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::avg(columnSpecs));
}

AggregateWrapper* INTERNAL_wAvg(std::string weightColumn, std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::wavg(weightColumn, columnSpecs));
}

AggregateWrapper* INTERNAL_var(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::var(columnSpecs));
}

AggregateWrapper* INTERNAL_std(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::std(columnSpecs));
}

AggregateWrapper* INTERNAL_first(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::first(columnSpecs));
}

AggregateWrapper* INTERNAL_last(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::last(columnSpecs));
}

AggregateWrapper* INTERNAL_median(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::med(columnSpecs));
}

AggregateWrapper* INTERNAL_percentile(double percentile, std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::pct(percentile, false, columnSpecs));
}

AggregateWrapper* INTERNAL_count(std::string columnSpec) {
    return new AggregateWrapper(deephaven::client::Aggregate::count(columnSpec));
}


class TableHandleWrapper {
public:
    TableHandleWrapper(deephaven::client::TableHandle ref_table) :
        internal_tbl_hdl(std::move(ref_table)) {};

    // TABLE OPERATIONS

    // FILTER OPERATIONS

    TableHandleWrapper* select(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.select(columnSpecs));
    };

    TableHandleWrapper* view(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.view(columnSpecs));
    };

    TableHandleWrapper* update(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.update(columnSpecs));
    };

    TableHandleWrapper* updateView(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.updateView(columnSpecs));
    };

    TableHandleWrapper* dropColumns(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.dropColumns(columnSpecs));
    };

    TableHandleWrapper* where(std::string condition) {
        return new TableHandleWrapper(internal_tbl_hdl.where(condition));
    };

    // AGGREGATION OPERATIONS

    TableHandleWrapper* aggBy(Rcpp::List aggregations) {
        std::vector<deephaven::client::Aggregate> converted_aggregations = convertRcppListToVectorOfTypeAggregate(aggregations);
        return new TableHandleWrapper(internal_tbl_hdl.by(deephaven::client::AggregateCombo::create(converted_aggregations)));
    }

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

    TableHandleWrapper* avgBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.avgBy(columnSpecs));
    };

    TableHandleWrapper* wAvgBy(std::vector<std::string> columnSpecs, std::string weightColumn) {
        return new TableHandleWrapper(internal_tbl_hdl.wAvgBy(weightColumn, columnSpecs));
    };

    TableHandleWrapper* varBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.varBy(columnSpecs));
    };

    TableHandleWrapper* stdBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.stdBy(columnSpecs));
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

    TableHandleWrapper* percentileBy(std::vector<std::string> columnSpecs, double percentile) {
        return new TableHandleWrapper(internal_tbl_hdl.percentileBy(percentile, columnSpecs));
    };

    TableHandleWrapper* countBy(std::vector<std::string> columnSpecs, std::string countByColumn) {
        return new TableHandleWrapper(internal_tbl_hdl.countBy(countByColumn, columnSpecs));
    };

    TableHandleWrapper* headBy(std::vector<std::string> columnSpecs, int64_t n) {
        return new TableHandleWrapper(internal_tbl_hdl.headBy(n, columnSpecs));
    };

    TableHandleWrapper* tailBy(std::vector<std::string> columnSpecs, int64_t n) {
        return new TableHandleWrapper(internal_tbl_hdl.tailBy(n, columnSpecs));
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

    TableHandleWrapper* merge(std::string keyColumn, Rcpp::List sources) {
        std::vector<deephaven::client::TableHandle> converted_sources = convertRcppListToVectorOfTypeTableHandle(sources);
        return new TableHandleWrapper(internal_tbl_hdl.merge(keyColumn, converted_sources));
    };

    TableHandleWrapper* sort(std::vector<std::string> columnSpecs, std::vector<bool> descending) {
        std::vector<deephaven::client::SortPair> sort_pairs;
        sort_pairs.reserve(columnSpecs.size());
        if (descending.size() == 1) {
            if (descending[0] == false) {
                for(int i = 0; i < columnSpecs.size(); i++) {
                    sort_pairs.push_back(deephaven::client::SortPair::ascending(columnSpecs[i], false));
                }
            } else {
                for(int i = 0; i < columnSpecs.size(); i++) {
                    sort_pairs.push_back(deephaven::client::SortPair::descending(columnSpecs[i], false));
                }
            }
        } else {
            for(int i = 0; i < columnSpecs.size(); i++) {
                if (descending[i] == false) {
                    sort_pairs.push_back(deephaven::client::SortPair::ascending(columnSpecs[i], false));
                } else {
                    sort_pairs.push_back(deephaven::client::SortPair::descending(columnSpecs[i], false));
                }
            }
        }
        return new TableHandleWrapper(internal_tbl_hdl.sort(sort_pairs));
    };

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
    friend std::vector<deephaven::client::TableHandle> convertRcppListToVectorOfTypeTableHandle(Rcpp::List rcpp_list);
};

// ######################### conversion function for the above class
std::vector<deephaven::client::TableHandle> convertRcppListToVectorOfTypeTableHandle(Rcpp::List rcpp_list) {
    std::vector<deephaven::client::TableHandle> converted_list;
    converted_list.reserve(rcpp_list.size());

    for(int i = 0; i < rcpp_list.size(); i++) {
        Rcpp::Environment rcpp_list_element = rcpp_list[i];
        Rcpp::XPtr<TableHandleWrapper> xptr(rcpp_list_element.get(".pointer"));
        deephaven::client::TableHandle internal_tbl_hdl = xptr->internal_tbl_hdl;
        converted_list.push_back(internal_tbl_hdl);
    }
    return converted_list;
}


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
    const deephaven::client::Client internal_client;
    const deephaven::client::TableHandleManager internal_tbl_hdl_mngr = internal_client.getManager();
};


// ######################### RCPP GLUE #########################

using namespace Rcpp;

RCPP_EXPOSED_CLASS(ClientOptionsWrapper)
RCPP_EXPOSED_CLASS(TableHandleWrapper)
RCPP_EXPOSED_CLASS(AggregateWrapper)
RCPP_EXPOSED_CLASS(SortPairWrapper)
RCPP_EXPOSED_CLASS(ArrowArrayStream)

RCPP_MODULE(DeephavenInternalModule) {

    class_<AggregateWrapper>("INTERNAL_Aggregate")
    ;
    function("INTERNAL_min", &INTERNAL_min);
    function("INTERNAL_max", &INTERNAL_max);
    function("INTERNAL_sum", &INTERNAL_sum);
    function("INTERNAL_abs_sum", &INTERNAL_absSum);
    function("INTERNAL_avg", &INTERNAL_avg);
    function("INTERNAL_w_avg", &INTERNAL_wAvg);
    function("INTERNAL_var", &INTERNAL_var);
    function("INTERNAL_std", &INTERNAL_std);
    function("INTERNAL_first", &INTERNAL_first);
    function("INTERNAL_last", &INTERNAL_last);
    function("INTERNAL_median", &INTERNAL_median);
    function("INTERNAL_percentile", &INTERNAL_percentile);
    function("INTERNAL_count", &INTERNAL_count);


    class_<SortPairWrapper>("INTERNAL_SortPair")
    ;
    function("INTERNAL_sort_asc", &INTERNAL_sortAsc);
    function("INTERNAL_sort_desc", &INTERNAL_sortDesc);


    class_<TableHandleWrapper>("INTERNAL_TableHandle")
    .method("select", &TableHandleWrapper::select)
    .method("view", &TableHandleWrapper::view)
    .method("update", &TableHandleWrapper::update)
    .method("update_view", &TableHandleWrapper::updateView)
    .method("drop_columns", &TableHandleWrapper::dropColumns)
    .method("where", &TableHandleWrapper::where)

    .method("agg_by", &TableHandleWrapper::aggBy)
    .method("min_by", &TableHandleWrapper::minBy)
    .method("max_by", &TableHandleWrapper::maxBy)
    .method("sum_by", &TableHandleWrapper::sumBy)
    .method("abs_sum_by", &TableHandleWrapper::absSumBy)
    .method("avg_by", &TableHandleWrapper::avgBy)
    .method("w_avg_by", &TableHandleWrapper::wAvgBy)
    .method("var_by", &TableHandleWrapper::varBy)
    .method("std_by", &TableHandleWrapper::stdBy)
    .method("first_by", &TableHandleWrapper::firstBy)
    .method("last_by", &TableHandleWrapper::lastBy)
    .method("median_by", &TableHandleWrapper::medianBy)
    .method("percentile_by", &TableHandleWrapper::percentileBy)
    .method("count_by", &TableHandleWrapper::countBy)
    .method("head_by", &TableHandleWrapper::headBy)
    .method("tail_by", &TableHandleWrapper::tailBy)

    .method("cross_join", &TableHandleWrapper::crossJoin)
    .method("natural_join", &TableHandleWrapper::naturalJoin)
    .method("exact_join", &TableHandleWrapper::exactJoin)

    .method("head", &TableHandleWrapper::head)
    .method("tail", &TableHandleWrapper::tail)
    .method("ungroup", &TableHandleWrapper::ungroup)
    .method("merge", &TableHandleWrapper::merge)
    .method("sort", &TableHandleWrapper::sort)

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
    .method("empty_table", &ClientWrapper::emptyTable)
    .method("time_table", &ClientWrapper::timeTable)
    .method("check_for_table", &ClientWrapper::checkForTable)
    .method("run_script", &ClientWrapper::runScript)
    .method("new_arrow_array_stream_ptr", &ClientWrapper::newArrowArrayStreamPtr)
    .method("new_table_from_arrow_array_stream_ptr", &ClientWrapper::newTableFromArrowArrayStreamPtr)
    ;

}
