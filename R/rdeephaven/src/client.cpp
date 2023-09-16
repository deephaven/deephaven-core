/*
 * Most of the methods here wrap methods defined in client.h and client_options.h to expose them to R via Rcpp.
 * Thus, the only methods that are documented here are the ones that are unique to these classes, and not already
 * documented in one of the header files mentioned above.
 */

#include <iostream>
#include <memory>
#include <stdexcept>
#include <cstddef>
#include <string>
#include <utility>
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
            internal_agg_op(std::move(aggregate)) {}
private:
    deephaven::client::Aggregate internal_agg_op;
    friend TableHandleWrapper;
    friend std::vector<deephaven::client::Aggregate> convertRcppListToVectorOfTypeAggregate(Rcpp::List rcpp_list);
};

std::vector<deephaven::client::Aggregate> convertRcppListToVectorOfTypeAggregate(Rcpp::List rcpp_list) {
    std::vector<deephaven::client::Aggregate> converted_list;
    converted_list.reserve(rcpp_list.size());

    for(int i = 0; i < rcpp_list.size(); i++) {
        Rcpp::Environment rcpp_list_element = rcpp_list[i];
        Rcpp::XPtr<AggregateWrapper> xptr(rcpp_list_element.get(".pointer"));
        deephaven::client::Aggregate internal_agg_op = xptr->internal_agg_op;
        converted_list.push_back(internal_agg_op);
    }

    return converted_list;
}

AggregateWrapper* INTERNAL_agg_min(std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::Min(cols));
}

AggregateWrapper* INTERNAL_agg_max(std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::Max(cols));
}

AggregateWrapper* INTERNAL_agg_first(std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::First(cols));
}

AggregateWrapper* INTERNAL_agg_last(std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::Last(cols));
}

AggregateWrapper* INTERNAL_agg_sum(std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::Sum(cols));
}

AggregateWrapper* INTERNAL_agg_absSum(std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::AbsSum(cols));
}

AggregateWrapper* INTERNAL_agg_avg(std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::Avg(cols));
}

AggregateWrapper* INTERNAL_agg_wAvg(std::string weight_column, std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::WAvg(weight_column, cols));
}

AggregateWrapper* INTERNAL_agg_median(std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::Med(cols));
}

AggregateWrapper* INTERNAL_agg_var(std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::Var(cols));
}

AggregateWrapper* INTERNAL_agg_std(std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::Std(cols));
}

AggregateWrapper* INTERNAL_agg_percentile(double percentile, std::vector<std::string> cols) {
    return new AggregateWrapper(deephaven::client::Aggregate::Pct(percentile, false, cols));
}

AggregateWrapper* INTERNAL_agg_count(std::string col) {
    return new AggregateWrapper(deephaven::client::Aggregate::Count(col));
}


class TableHandleWrapper {
public:
    TableHandleWrapper(deephaven::client::TableHandle ref_table) :
        internal_tbl_hdl(std::move(ref_table)) {};

    TableHandleWrapper* Select(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.Select(cols));
    };

    TableHandleWrapper* View(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.View(cols));
    };

    TableHandleWrapper* Update(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.Update(cols));
    };

    TableHandleWrapper* UpdateView(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.UpdateView(cols));
    };

    TableHandleWrapper* DropColumns(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.DropColumns(cols));
    };

    TableHandleWrapper* Where(std::string condition) {
        return new TableHandleWrapper(internal_tbl_hdl.Where(condition));
    };

    TableHandleWrapper* GroupBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.By(cols));
    };

    TableHandleWrapper* Ungroup(std::vector<std::string> group_by_cols) {
        return new TableHandleWrapper(internal_tbl_hdl.Ungroup(false, group_by_cols));
    };

    TableHandleWrapper* AggBy(Rcpp::List aggregations, std::vector<std::string> group_by_columns) {
        std::vector<deephaven::client::Aggregate> converted_aggregations = convertRcppListToVectorOfTypeAggregate(aggregations);
        return new TableHandleWrapper(internal_tbl_hdl.By(deephaven::client::AggregateCombo::Create(converted_aggregations), group_by_columns));
    };

    TableHandleWrapper* AggAllBy(AggregateWrapper &aggregation, std::vector<std::string> group_by_columns) {
        std::vector<deephaven::client::Aggregate> converted_aggregation = {aggregation.internal_agg_op};
        return new TableHandleWrapper(internal_tbl_hdl.By(deephaven::client::AggregateCombo::Create(converted_aggregation), group_by_columns));
    };

    TableHandleWrapper* FirstBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.FirstBy(cols));
    };

    TableHandleWrapper* LastBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.LastBy(cols));
    };

    TableHandleWrapper* HeadBy(int64_t n, std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.HeadBy(n, cols));
    };

    TableHandleWrapper* TailBy(int64_t n, std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.TailBy(n, cols));
    };

    TableHandleWrapper* MinBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.MinBy(cols));
    };

    TableHandleWrapper* MaxBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.MaxBy(cols));
    };

    TableHandleWrapper* SumBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.SumBy(cols));
    };

    TableHandleWrapper* AbsSumBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.AbsSumBy(cols));
    };

    TableHandleWrapper* AvgBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.AvgBy(cols));
    };

    TableHandleWrapper* WAvgBy(std::string weight_column, std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.WAvgBy(weight_column, cols));
    };

    TableHandleWrapper* MedianBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.MedianBy(cols));
    };

    TableHandleWrapper* VarBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.VarBy(cols));
    };

    TableHandleWrapper* StdBy(std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.StdBy(cols));
    };

    TableHandleWrapper* PercentileBy(double percentile, std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.PercentileBy(percentile, cols));
    };

    TableHandleWrapper* CountBy(std::string count_by_col, std::vector<std::string> cols) {
        return new TableHandleWrapper(internal_tbl_hdl.CountBy(count_by_col, cols));
    };

    TableHandleWrapper* CrossJoin(const TableHandleWrapper &right_side, std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) {
        return new TableHandleWrapper(internal_tbl_hdl.CrossJoin(right_side.internal_tbl_hdl, columns_to_match, columns_to_add));
    };

    TableHandleWrapper* NaturalJoin(const TableHandleWrapper &right_side, std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) {
        return new TableHandleWrapper(internal_tbl_hdl.NaturalJoin(right_side.internal_tbl_hdl, columns_to_match, columns_to_add));
    };

    TableHandleWrapper* ExactJoin(const TableHandleWrapper &right_side, std::vector<std::string> columns_to_match, std::vector<std::string> columns_to_add) {
        return new TableHandleWrapper(internal_tbl_hdl.ExactJoin(right_side.internal_tbl_hdl, columns_to_match, columns_to_add));
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

    TableHandleWrapper* Sort(std::vector<std::string> cols, std::vector<bool> descending, std::vector<bool> abs_sort) {
        std::vector<deephaven::client::SortPair> sort_pairs;
        sort_pairs.reserve(cols.size());

        if (descending.size() == 1) {
            descending = std::vector<bool>(cols.size(), descending[0]);
        }

        if (abs_sort.size() == 1) {
            abs_sort = std::vector<bool>(cols.size(), abs_sort[0]);
        }

        for(std::size_t i = 0; i < cols.size(); i++) {
            if (!descending[i]) {
                sort_pairs.push_back(deephaven::client::SortPair::Ascending(cols[i], abs_sort[i]));
            } else {
                sort_pairs.push_back(deephaven::client::SortPair::Descending(cols[i], abs_sort[i]));
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

    void BindToVariable(std::string table_name) {
        internal_tbl_hdl.BindToVariable(table_name);
    }

    /**
     * Creates and returns a pointer to an ArrowArrayStream C struct containing the data from the table referenced by internal_tbl_hdl.
     * Intended to be used for creating an Arrow RecordBatchReader in R via RecordBatchReader$import_from_c(ptr).
    */
    SEXP GetArrowArrayStreamPtr() {

        std::shared_ptr<arrow::flight::FlightStreamReader> fsr = internal_tbl_hdl.GetFlightStreamReader();

        std::vector<std::shared_ptr<arrow::RecordBatch>> empty_record_batches;
        deephaven::client::utility::OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsr->ReadAll(&empty_record_batches)));

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

    void SetSessionType(const std::string &session_type) {
        internal_options->SetSessionType(session_type);
    }

    void SetUseTls(bool use_tls) {
        internal_options->SetUseTls(use_tls);
    }

    void SetTlsRootCerts(std::string tls_root_certs) {
        internal_options->SetTlsRootCerts(tls_root_certs);
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
        internal_client(
            Rcpp::XPtr<deephaven::client::Client>(
                new deephaven::client::Client(
                    std::move(
                        deephaven::client::Client::Connect(target, *client_options.internal_options)
                    )
                )
            )
        ) {}

    // We need the ability to create a ClientWrapper from the enterprise
    // client, when the underlying C++ object is already created.
    ClientWrapper(SEXP sexp) :
        internal_client(Rcpp::XPtr<ClientWrapper>(sexp)) {}

    SEXP InternalClient() {
      return internal_client;
    }

    TableHandleWrapper* OpenTable(std::string table_name) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.FetchTable(table_name));
    }

    TableHandleWrapper* EmptyTable(int64_t size) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.EmptyTable(size));
    }

    TableHandleWrapper* TimeTable(std::string period_ISO, std::string start_time_ISO) {
        if ((start_time_ISO == "now") || (start_time_ISO == "")) {
            return new TableHandleWrapper(internal_tbl_hdl_mngr.TimeTable(period_ISO));
        }
        return new TableHandleWrapper(internal_tbl_hdl_mngr.TimeTable(period_ISO, start_time_ISO));
    };

    TableHandleWrapper* MakeTableHandleFromTicket(std::string ticket) {
        return new TableHandleWrapper(internal_tbl_hdl_mngr.MakeTableHandleFromTicket(ticket));
    }

    void RunScript(std::string code) {
        internal_tbl_hdl_mngr.RunScript(code);
    }

    /**
     * Checks for the existence of a table named table_name on the server.
     * @param table_name Name of the table to search for.
     * @return Boolean indicating whether table_name exists on the server or not.
    */
    bool CheckForTable(std::string table_name) {
        // we have to first fetchTable to check existence, fetchTable does not fail on its own, but .observe() will fail if table doesn't exist
        deephaven::client::TableHandle table_handle = internal_tbl_hdl_mngr.FetchTable(table_name);
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

        deephaven::client::utility::OkOrThrow(DEEPHAVEN_LOCATION_EXPR(wrapper.FlightClient()->DoPut(options, fd, schema, &fsw, &fmr)));
        while(true) {
            std::shared_ptr<arrow::RecordBatch> this_batch;
            deephaven::client::utility::OkOrThrow(DEEPHAVEN_LOCATION_EXPR(record_batch_reader->ReadNext(&this_batch)));
            if (this_batch == nullptr) {
                break;
            }
            deephaven::client::utility::OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsw->WriteRecordBatch(*this_batch)));
        }
        deephaven::client::utility::OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsw->DoneWriting()));
        deephaven::client::utility::OkOrThrow(DEEPHAVEN_LOCATION_EXPR(fsw->Close()));

        auto new_tbl_hdl = internal_tbl_hdl_mngr.MakeTableHandleFromTicket(ticket);
        return new TableHandleWrapper(new_tbl_hdl);
    }

    void Close() {
        internal_client->Close();
    }

private:
    // We let R manage the lifetime of internal_client underlying C++ object,
    // according to its tracking of references.
    // We hold one here, but there may be other references in the case of the enterprise client.
    Rcpp::XPtr<deephaven::client::Client> internal_client;
    const deephaven::client::TableHandleManager internal_tbl_hdl_mngr = internal_client->GetManager();
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
    .method("agg_all_by", &TableHandleWrapper::AggAllBy)

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
    .constructor<SEXP>()
    .method("internal_client", &ClientWrapper::InternalClient)
    .method("open_table", &ClientWrapper::OpenTable)
    .method("empty_table", &ClientWrapper::EmptyTable)
    .method("time_table", &ClientWrapper::TimeTable)
    .method("check_for_table", &ClientWrapper::CheckForTable)
    .method("make_table_handle_from_ticket", &ClientWrapper::MakeTableHandleFromTicket)
    .method("run_script", &ClientWrapper::RunScript)
    .method("new_arrow_array_stream_ptr", &ClientWrapper::NewArrowArrayStreamPtr)
    .method("new_table_from_arrow_array_stream_ptr", &ClientWrapper::NewTableFromArrowArrayStreamPtr)
    .method("close", &ClientWrapper::Close)
    ;
}
