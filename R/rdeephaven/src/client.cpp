/*
 * Most of the methods here wrap methods defined in client.h and client_options.h to expose them to R via Rcpp.
 * Thus, the only methods that are documented here are the ones that are unique to these classes, and not already
 * documented in one of the header files mentioned above.
 */

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

using deephaven::dhcore::utility::Base64Encode;

// forward declaration of classes
class AggregateWrapper;
class TableHandleWrapper;
class ClientOptionsWrapper;
class ClientWrapper;

// forward declaration of conversion functions
std::vector<deephaven::client::Aggregate> convertRcppListToVectorOfTypeAggregate(Rcpp::List rcpp_list);
std::vector<deephaven::client::TableHandle> convertRcppListToVectorOfTypeTableHandle(Rcpp::List rcpp_list);


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

AggregateWrapper* INTERNAL_agg_min(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::Min(columnSpecs));
}

AggregateWrapper* INTERNAL_agg_max(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::Max(columnSpecs));
}

AggregateWrapper* INTERNAL_agg_first(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::First(columnSpecs));
}

AggregateWrapper* INTERNAL_agg_last(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::Last(columnSpecs));
}

AggregateWrapper* INTERNAL_agg_sum(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::Sum(columnSpecs));
}

AggregateWrapper* INTERNAL_agg_absSum(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::AbsSum(columnSpecs));
}

AggregateWrapper* INTERNAL_agg_avg(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::Avg(columnSpecs));
}

AggregateWrapper* INTERNAL_agg_wAvg(std::string weightColumn, std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::WAvg(weightColumn, columnSpecs));
}

AggregateWrapper* INTERNAL_agg_median(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::Med(columnSpecs));
}

AggregateWrapper* INTERNAL_agg_var(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::Var(columnSpecs));
}

AggregateWrapper* INTERNAL_agg_std(std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::Std(columnSpecs));
}

AggregateWrapper* INTERNAL_agg_percentile(double percentile, std::vector<std::string> columnSpecs) {
    return new AggregateWrapper(deephaven::client::Aggregate::Pct(percentile, false, columnSpecs));
}

AggregateWrapper* INTERNAL_agg_count(std::string columnSpec) {
    return new AggregateWrapper(deephaven::client::Aggregate::Count(columnSpec));
}


class TableHandleWrapper {
public:
    TableHandleWrapper(deephaven::client::TableHandle ref_table) :
            internal_tbl_hdl(std::move(ref_table)) {};

    TableHandleWrapper* Select(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.Select(columnSpecs));
    };

    TableHandleWrapper* View(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.View(columnSpecs));
    };

    TableHandleWrapper* Update(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.Update(columnSpecs));
    };

    TableHandleWrapper* UpdateView(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.UpdateView(columnSpecs));
    };

    TableHandleWrapper* DropColumns(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.DropColumns(columnSpecs));
    };

    TableHandleWrapper* Where(std::string condition) {
        return new TableHandleWrapper(internal_tbl_hdl.Where(condition));
    };

    TableHandleWrapper* GroupBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.By(columnSpecs));
    };

    TableHandleWrapper* Ungroup(std::vector<std::string> groupByColumns) {
        return new TableHandleWrapper(internal_tbl_hdl.Ungroup(false, groupByColumns));
    };

    TableHandleWrapper* AggBy(Rcpp::List aggregations, std::vector<std::string> groupByColumns) {
        std::vector<deephaven::client::Aggregate> converted_aggregations = convertRcppListToVectorOfTypeAggregate(aggregations);
        return new TableHandleWrapper(internal_tbl_hdl.By(deephaven::client::AggregateCombo::Create(converted_aggregations), groupByColumns));
    }

    TableHandleWrapper* FirstBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.FirstBy(columnSpecs));
    };

    TableHandleWrapper* LastBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.LastBy(columnSpecs));
    };

    TableHandleWrapper* HeadBy(int64_t n, std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.HeadBy(n, columnSpecs));
    };

    TableHandleWrapper* TailBy(int64_t n, std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.TailBy(n, columnSpecs));
    };

    TableHandleWrapper* MinBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.MinBy(columnSpecs));
    };

    TableHandleWrapper* MaxBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.MaxBy(columnSpecs));
    };

    TableHandleWrapper* SumBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.SumBy(columnSpecs));
    };

    TableHandleWrapper* AbsSumBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.AbsSumBy(columnSpecs));
    };

    TableHandleWrapper* AvgBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.AvgBy(columnSpecs));
    };

    TableHandleWrapper* WAvgBy(std::string weightColumn, std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.WAvgBy(weightColumn, columnSpecs));
    };

    TableHandleWrapper* MedianBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.MedianBy(columnSpecs));
    };

    TableHandleWrapper* VarBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.VarBy(columnSpecs));
    };

    TableHandleWrapper* StdBy(std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.StdBy(columnSpecs));
    };

    TableHandleWrapper* PercentileBy(double percentile, std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.PercentileBy(percentile, columnSpecs));
    };

    TableHandleWrapper* CountBy(std::string countByColumn, std::vector<std::string> columnSpecs) {
        return new TableHandleWrapper(internal_tbl_hdl.CountBy(countByColumn, columnSpecs));
    };

    TableHandleWrapper* CrossJoin(const TableHandleWrapper &rightSide, std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) {
        return new TableHandleWrapper(internal_tbl_hdl.CrossJoin(rightSide.internal_tbl_hdl, columnsToMatch, columnsToAdd));
    };

    TableHandleWrapper* NaturalJoin(const TableHandleWrapper &rightSide, std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) {
        return new TableHandleWrapper(internal_tbl_hdl.NaturalJoin(rightSide.internal_tbl_hdl, columnsToMatch, columnsToAdd));
    };

    TableHandleWrapper* ExactJoin(const TableHandleWrapper &rightSide, std::vector<std::string> columnsToMatch, std::vector<std::string> columnsToAdd) {
        return new TableHandleWrapper(internal_tbl_hdl.ExactJoin(rightSide.internal_tbl_hdl, columnsToMatch, columnsToAdd));
    };

    TableHandleWrapper* Head(int64_t n) {
        return new TableHandleWrapper(internal_tbl_hdl.Head(n));
    };

    TableHandleWrapper* Tail(int64_t n) {
        return new TableHandleWrapper(internal_tbl_hdl.Tail(n));
    };

    TableHandleWrapper* Merge(Rcpp::List sources) {
        std::vector<deephaven::client::TableHandle> converted_sources = convertRcppListToVectorOfTypeTableHandle(sources);
        return new TableHandleWrapper(internal_tbl_hdl.Merge(converted_sources));
    };

    TableHandleWrapper* Sort(std::vector<std::string> columnSpecs, std::vector<bool> descending, std::vector<bool> absSort) {
        std::vector<deephaven::client::SortPair> sort_pairs;
        sort_pairs.reserve(columnSpecs.size());

        if (descending.size() == 1) {
            descending = std::vector<bool>(columnSpecs.size(), descending[0]);
        }

        if (absSort.size() == 1) {
            absSort = std::vector<bool>(columnSpecs.size(), absSort[0]);
        }

        for(int i = 0; i < columnSpecs.size(); i++) {
            if (!descending[i]) {
                sort_pairs.push_back(deephaven::client::SortPair::Ascending(columnSpecs[i], absSort[i]));
            } else {
                sort_pairs.push_back(deephaven::client::SortPair::Descending(columnSpecs[i], absSort[i]));
            }
        }

        return new TableHandleWrapper(internal_tbl_hdl.Sort(sort_pairs));
    };

    bool IsStatic() {
        return internal_tbl_hdl.IsStatic();
    }

    int64_t NumRows() {
        return internal_tbl_hdl.NumRows();
    }

    int64_t NumCols() {
        return internal_tbl_hdl.Schema()->NumCols();
    }

    void BindToVariable(std::string tableName) {
        internal_tbl_hdl.BindToVariable(tableName);
    }

    /**
     * Creates and returns a pointer to an ArrowArrayStream C struct containing the data from the table referenced by internal_tbl_hdl.
     * Intended to be used for creating an Arrow RecordBatchReader in R via RecordBatchReader$import_from_c(ptr).
    */
    SEXP GetArrowArrayStreamPtr() {

        std::shared_ptr<arrow::flight::FlightStreamReader> fsr = internal_tbl_hdl.GetFlightStreamReader();

        std::vector<std::shared_ptr<arrow::RecordBatch>> empty_record_batches;
        deephaven::client::utility::OkOrThrow(DEEPHAVEN_EXPR_MSG(fsr->ReadAll(&empty_record_batches)));

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


class ClientOptionsWrapper {
public:

    ClientOptionsWrapper() :
            internal_options(std::make_shared<deephaven::client::ClientOptions>()) {}

    void SetDefaultAuthentication() {
        internal_options->SetDefaultAuthentication();
    }

    void SetBasicAuthentication(const std::string &authentication_token) {
        const std::string authentication_token_base64 = Base64Encode(authentication_token);
        internal_options->SetCustomAuthentication("Basic", authentication_token_base64);
    }

    void SetCustomAuthentication(const std::string &authentication_type, const std::string &authentication_token) {
        internal_options->SetCustomAuthentication(authentication_type, authentication_token);
    }

    void SetSessionType(const std::string &sessionType) {
        internal_options->SetSessionType(sessionType);
    }

    void SetUseTls(bool useTls) {
        internal_options->SetUseTls(useTls);
    }

    void SetTlsRootCerts(std::string tlsRootCerts) {
        internal_options->SetTlsRootCerts(tlsRootCerts);
    }

    void AddIntOption(std::string opt, int val) {
        internal_options->AddIntOption(opt, val);
    }

    void AddStringOption(std::string opt, std::string val) {
        internal_options->AddStringOption(opt, val);
    }

    void AddExtraHeader(std::string header_name, std::string header_value) {
        internal_options->AddExtraHeader(header_name, header_value);
    }

private:
    std::shared_ptr<deephaven::client::ClientOptions> internal_options;
    friend ClientWrapper;
};


class ClientWrapper {
public:

    ClientWrapper(std::string target, const ClientOptionsWrapper &client_options) :
            internal_client(deephaven::client::Client::Connect(target, *client_options.internal_options)) {}

    TableHandleWrapper* OpenTable(std::string tableName) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.FetchTable(tableName));
    }

    TableHandleWrapper* EmptyTable(int64_t size) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.EmptyTable(size));
    }

    TableHandleWrapper* TimeTable(std::string periodISO, std::string startTimeISO) {
        if((startTimeISO == "now") || (startTimeISO == "")) {
            return new TableHandleWrapper(internal_tbl_hdl_mngr.TimeTable(periodISO));
        }
        return new TableHandleWrapper(internal_tbl_hdl_mngr.TimeTable(periodISO, startTimeISO));
    };

    void RunScript(std::string code) {
        internal_tbl_hdl_mngr.RunScript(code);
    }

    /**
     * Checks for the existence of a table named tableName on the server.
     * @param tableName Name of the table to search for.
     * @return Boolean indicating whether tableName exists on the server or not.
    */
    bool CheckForTable(std::string tableName) {
        // we have to first fetchTable to check existence, fetchTable does not fail on its own, but .observe() will fail if table doesn't exist
        deephaven::client::TableHandle table_handle = internal_tbl_hdl_mngr.FetchTable(tableName);
        try {
            table_handle.Observe();
        } catch(...) {
            return false;
        }
        return true;
    }

    /**
     * Allocates memory for an ArrowArrayStream C struct and returns a pointer to the new chunk of memory.
     * Intended to be used to get a pointer to pass to Arrow's R library RecordBatchReader$export_to_c(ptr).
    */
    SEXP NewArrowArrayStreamPtr() {
        ArrowArrayStream* stream_ptr = new ArrowArrayStream();
        return Rcpp::XPtr<ArrowArrayStream>(stream_ptr, true);
    }

    /**
     * Uses a pointer to a populated ArrowArrayStream C struct to create a new table on the server from the data in the C struct.
     * @param stream_ptr Pointer to an existing and populated ArrayArrayStream, populated by a call to RecordBatchReader$export_to_c(ptr) from R.
    */
    TableHandleWrapper* NewTableFromArrowArrayStreamPtr(Rcpp::XPtr<ArrowArrayStream> stream_ptr) {

        auto wrapper = internal_tbl_hdl_mngr.CreateFlightWrapper();
        arrow::flight::FlightCallOptions options;
        wrapper.AddHeaders(&options);

        // extract RecordBatchReader from the struct pointed to by the passed stream_ptr
        std::shared_ptr<arrow::RecordBatchReader> record_batch_reader = arrow::ImportRecordBatchReader(stream_ptr.get()).ValueOrDie();
        auto schema = record_batch_reader.get()->schema();

        // write RecordBatchReader data to table on server with DoPut
        std::unique_ptr<arrow::flight::FlightStreamWriter> fsw;
        std::unique_ptr<arrow::flight::FlightMetadataReader> fmr;

        auto ticket = internal_tbl_hdl_mngr.NewTicket();
        auto fd = deephaven::client::utility::ConvertTicketToFlightDescriptor(ticket);

        deephaven::client::utility::OkOrThrow(DEEPHAVEN_EXPR_MSG(wrapper.FlightClient()->DoPut(options, fd, schema, &fsw, &fmr)));
        while(true) {
            std::shared_ptr<arrow::RecordBatch> this_batch;
            deephaven::client::utility::OkOrThrow(DEEPHAVEN_EXPR_MSG(record_batch_reader->ReadNext(&this_batch)));
            if (this_batch == nullptr) {
                break;
            }
            deephaven::client::utility::OkOrThrow(DEEPHAVEN_EXPR_MSG(fsw->WriteRecordBatch(*this_batch)));
        }
        deephaven::client::utility::OkOrThrow(DEEPHAVEN_EXPR_MSG(fsw->DoneWriting()));
        deephaven::client::utility::OkOrThrow(DEEPHAVEN_EXPR_MSG(fsw->Close()));

        auto new_tbl_hdl = internal_tbl_hdl_mngr.MakeTableHandleFromTicket(ticket);
        return new TableHandleWrapper(new_tbl_hdl);
    }

    void Close() {
        internal_client.Close();
    }

private:
    deephaven::client::Client internal_client;
    const deephaven::client::TableHandleManager internal_tbl_hdl_mngr = internal_client.GetManager();
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
    function("INTERNAL_agg_first", &INTERNAL_agg_first);
    function("INTERNAL_agg_last", &INTERNAL_agg_last);
    function("INTERNAL_agg_min", &INTERNAL_agg_min);
    function("INTERNAL_agg_max", &INTERNAL_agg_max);
    function("INTERNAL_agg_sum", &INTERNAL_agg_sum);
    function("INTERNAL_agg_abs_sum", &INTERNAL_agg_absSum);
    function("INTERNAL_agg_avg", &INTERNAL_agg_avg);
    function("INTERNAL_agg_w_avg", &INTERNAL_agg_wAvg);
    function("INTERNAL_agg_median", &INTERNAL_agg_median);
    function("INTERNAL_agg_var", &INTERNAL_agg_var);
    function("INTERNAL_agg_std", &INTERNAL_agg_std);
    function("INTERNAL_agg_percentile", &INTERNAL_agg_percentile);
    function("INTERNAL_agg_count", &INTERNAL_agg_count);


    class_<TableHandleWrapper>("INTERNAL_TableHandle")
    .method("select", &TableHandleWrapper::Select)
    .method("view", &TableHandleWrapper::View)
    .method("update", &TableHandleWrapper::Update)
    .method("update_view", &TableHandleWrapper::UpdateView)
    .method("drop_columns", &TableHandleWrapper::DropColumns)
    .method("where", &TableHandleWrapper::Where)

    .method("group_by", &TableHandleWrapper::GroupBy)
    .method("ungroup", &TableHandleWrapper::Ungroup)

    .method("agg_by", &TableHandleWrapper::AggBy)
    .method("first_by", &TableHandleWrapper::FirstBy)
    .method("last_by", &TableHandleWrapper::LastBy)
    .method("head_by", &TableHandleWrapper::HeadBy)
    .method("tail_by", &TableHandleWrapper::TailBy)
    .method("min_by", &TableHandleWrapper::MinBy)
    .method("max_by", &TableHandleWrapper::MaxBy)
    .method("sum_by", &TableHandleWrapper::SumBy)
    .method("abs_sum_by", &TableHandleWrapper::AbsSumBy)
    .method("avg_by", &TableHandleWrapper::AvgBy)
    .method("w_avg_by", &TableHandleWrapper::WAvgBy)
    .method("median_by", &TableHandleWrapper::MedianBy)
    .method("var_by", &TableHandleWrapper::VarBy)
    .method("std_by", &TableHandleWrapper::StdBy)
    .method("percentile_by", &TableHandleWrapper::PercentileBy)
    .method("count_by", &TableHandleWrapper::CountBy)

    .method("cross_join", &TableHandleWrapper::CrossJoin)
    .method("natural_join", &TableHandleWrapper::NaturalJoin)
    .method("exact_join", &TableHandleWrapper::ExactJoin)

    .method("head", &TableHandleWrapper::Head)
    .method("tail", &TableHandleWrapper::Tail)
    .method("merge", &TableHandleWrapper::Merge)
    .method("sort", &TableHandleWrapper::Sort)

    .method("is_static", &TableHandleWrapper::IsStatic)
    .method("num_rows", &TableHandleWrapper::NumRows)
    .method("num_cols", &TableHandleWrapper::NumCols)
    .method("bind_to_variable", &TableHandleWrapper::BindToVariable)
    .method("get_arrow_array_stream_ptr", &TableHandleWrapper::GetArrowArrayStreamPtr)
    ;


    class_<ClientOptionsWrapper>("INTERNAL_ClientOptions")
    .constructor()
    .method("set_default_authentication", &ClientOptionsWrapper::SetDefaultAuthentication)
    .method("set_basic_authentication", &ClientOptionsWrapper::SetBasicAuthentication)
    .method("set_custom_authentication", &ClientOptionsWrapper::SetCustomAuthentication)
    .method("set_session_type", &ClientOptionsWrapper::SetSessionType)
    .method("set_use_tls", &ClientOptionsWrapper::SetUseTls)
    .method("set_tls_root_certs", &ClientOptionsWrapper::SetTlsRootCerts)
    .method("add_int_option", &ClientOptionsWrapper::AddIntOption)
    .method("add_string_option", &ClientOptionsWrapper::AddStringOption)
    .method("add_extra_header", &ClientOptionsWrapper::AddExtraHeader)
    ;


    class_<ClientWrapper>("INTERNAL_Client")
    .constructor<std::string, const ClientOptionsWrapper&>()
    .method("open_table", &ClientWrapper::OpenTable)
    .method("empty_table", &ClientWrapper::EmptyTable)
    .method("time_table", &ClientWrapper::TimeTable)
    .method("check_for_table", &ClientWrapper::CheckForTable)
    .method("run_script", &ClientWrapper::RunScript)
    .method("new_arrow_array_stream_ptr", &ClientWrapper::NewArrowArrayStreamPtr)
    .method("new_table_from_arrow_array_stream_ptr", &ClientWrapper::NewTableFromArrowArrayStreamPtr)
    .method("close", &ClientWrapper::Close)
    ;

}
