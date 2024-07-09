/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <cstdint>
#include "deephaven/client/client.h"
#include "deephaven/client/client_options.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/dhcore/interop/interop_util.h"


namespace deephaven::client::interop {
/**
 * A thin wrapper about std::shared_ptr<ArrowTable>. Not really necessary but
 * makes it easier for humans to map to the corresponding class on the C# side.
 */
struct ArrowTableSpWrapper {
public:
  explicit ArrowTableSpWrapper(std::shared_ptr<arrow::Table> table) : table_(std::move(table)) {}
  std::shared_ptr<arrow::Table> table_;
};

/**
 * This class exists so we don't get confused about what we are passing
 * back and forth to .NET. Basically, like any other object, we need to
 * heap-allocate this object and pass an opaque pointer back and forth
 * to .NET. The fact that this object's only member is a shared pointer
 * is irrelevant in terms of what we need to do.
 */
struct ClientTableSpWrapper {
  using ClientTable = deephaven::dhcore::clienttable::ClientTable;
public:
  explicit ClientTableSpWrapper(std::shared_ptr<ClientTable> table) : table_(std::move(table)) {}
  std::shared_ptr<ClientTable> table_;
};
}  // namespace deephaven::client::interop {

extern "C" {
void deephaven_client_TableHandleManager_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandleManager> self);

void deephaven_client_TableHandleManager_EmptyTable(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandleManager> self,
    int64_t size,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandleManager_FetchTable(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandleManager> self,
    const char *table_name,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandleManager_TimeTable(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandleManager> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::DurationSpecifier> period,
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TimePointSpecifier> start_time,
    deephaven::dhcore::interop::InteropBool blink_table,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandleManager_InputTable(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandleManager> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> initial_table,
    const char **key_columns, int32_t num_key_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandleManager_RunScript(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandleManager> self,
    const char *code,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_Client_Connect(const char *target,
    deephaven::dhcore::interop::NativePtr<deephaven::client::ClientOptions> options,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Client> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_Client_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::Client> self);

void deephaven_client_Client_Close(
    deephaven::dhcore::interop::NativePtr<deephaven::client::Client> self,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_Client_GetManager(
    deephaven::dhcore::interop::NativePtr<deephaven::client::Client> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandleManager> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_GetAttributes(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    int32_t *num_columns, int64_t *num_rows,
    deephaven::dhcore::interop::InteropBool *is_static,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_GetSchema(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    int32_t num_columns,
    deephaven::dhcore::interop::StringHandle *columns, int32_t *column_types,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self);

void deephaven_client_TableHandle_GetManager(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandleManager> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Where(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char *condition,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Ungroup(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::InteropBool null_fill,
    const char **group_by_columns, int32_t num_group_by_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Sort(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **columns,
    const deephaven::dhcore::interop::InteropBool *ascendings,
    const deephaven::dhcore::interop::InteropBool *abss,
    int32_t num_sort_pairs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Select(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_SelectDistinct(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_View(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_DropColumns(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Update(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_LazyUpdate(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_LastBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Head(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    int64_t num_rows,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Tail(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    int64_t num_rows,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Merge(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char *key_column,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *sources,
    int32_t num_sources,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_CrossJoin(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> right_side,
    const char **columns_to_match, int32_t num_columns_to_match,
    const char **columns_to_add, int32_t num_columns_to_add,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_NaturalJoin(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> right_side,
    const char **columns_to_match, int32_t num_columns_to_match,
    const char **columns_to_add, int32_t num_columns_to_add,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_LeftOuterJoin(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> right_side,
    const char **columns_to_match, int32_t num_columns_to_match,
    const char **columns_to_add, int32_t num_columns_to_add,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_ExactJoin(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> right_side,
    const char **columns_to_match, int32_t num_columns_to_match,
    const char **columns_to_add, int32_t num_columns_to_add,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Aj(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> right_side,
    const char **columns_to_match, int32_t num_columns_to_match,
    const char **columns_to_add, int32_t num_columns_to_add,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Raj(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> right_side,
    const char **columns_to_match, int32_t num_columns_to_match,
    const char **columns_to_add, int32_t num_columns_to_add,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_UpdateBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const deephaven::dhcore::interop::NativePtr<deephaven::client::UpdateByOperation> *ops,
    int32_t num_ops,
    const char **by, int32_t num_by,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_WhereIn(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> filter_table,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_AddTable(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> table_to_add,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_RemoveTable(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> table_to_remove,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_By(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::AggregateCombo> aggregate_combo,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_MinBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_MaxBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_SumBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_AbsSumBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_VarBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_StdBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_AvgBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_LastBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_FirstBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_MedianBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_PercentileBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    double percentile, deephaven::dhcore::interop::InteropBool avg_median,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_CountBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char *weightColumn,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_WavgBy(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char *weightColumn,
    const char **column_specs, int32_t num_column_specs,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_BindToVariable(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    const char *variable,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_ToClientTable(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> *client_table,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_ToString(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::InteropBool want_headers,
    deephaven::dhcore::interop::StringHandle *result_handle,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_ToArrowTable(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ArrowTableSpWrapper> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

using NativeOnUpdate = void(
    deephaven::dhcore::interop::NativePtr<deephaven::dhcore::ticking::TickingUpdate> ticking_update);
using NativeOnFailure = void(deephaven::dhcore::interop::StringHandle,
    deephaven::dhcore::interop::StringPoolHandle);

void deephaven_client_TableHandle_Subscribe(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<NativeOnUpdate> native_on_update,
    deephaven::dhcore::interop::NativePtr<NativeOnFailure> native_on_failure,
    deephaven::dhcore::interop::NativePtr<std::shared_ptr<deephaven::client::subscription::SubscriptionHandle>> *native_subscription_handle,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TableHandle_Unsubscribe(
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> self,
    deephaven::dhcore::interop::NativePtr<std::shared_ptr<deephaven::client::subscription::SubscriptionHandle>> native_subscription_handle,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_SubscriptionHandle_dtor(
    deephaven::dhcore::interop::NativePtr<std::shared_ptr<deephaven::client::subscription::SubscriptionHandle>> self);

void deephaven_client_ArrowTable_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ArrowTableSpWrapper> self);

void deephaven_client_ArrowTable_GetDimensions(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ArrowTableSpWrapper> self,
    int32_t *num_columns, int64_t *num_rows,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_ArrowTable_GetSchema(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ArrowTableSpWrapper> self,
    int32_t num_columns,
    deephaven::dhcore::interop::StringHandle *column_handles, int32_t *column_types,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_ClientTable_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self);

void deephaven_client_ClientTable_GetDimensions(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t *num_columns, int64_t *num_rows,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_ClientTable_Schema(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t num_columns,
    deephaven::dhcore::interop::StringHandle *column_handles,
    int32_t *column_types,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_ClientTable_ToString(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    deephaven::dhcore::interop::InteropBool want_headers,
    deephaven::dhcore::interop::InteropBool want_row_numbers,
    deephaven::dhcore::interop::StringHandle *text_handle,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_ClientTableHelper_GetInt8Column(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t column_index, int8_t *data,
    deephaven::dhcore::interop::InteropBool *optional_dest_null_flags, int64_t num_rows,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientTableHelper_GetInt16Column(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t column_index, int16_t *data,
    deephaven::dhcore::interop::InteropBool *optional_dest_null_flags, int64_t num_rows,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientTableHelper_GetInt32Column(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t column_index, int32_t *data,
    deephaven::dhcore::interop::InteropBool *optional_dest_null_flags, int64_t num_rows,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientTableHelper_GetInt64Column(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t column_index, int64_t *data,
    deephaven::dhcore::interop::InteropBool *optional_dest_null_flags, int64_t num_rows,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientTableHelper_GetFloatColumn(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t column_index, float *data,
    deephaven::dhcore::interop::InteropBool *optional_dest_null_flags, int64_t num_rows,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientTableHelper_GetDoubleColumn(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t column_index, double *data,
    deephaven::dhcore::interop::InteropBool *optional_dest_null_flags,
    int64_t num_rows,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientTableHelper_GetCharAsInt16Column(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t column_index, int16_t *data,
    deephaven::dhcore::interop::InteropBool *optional_dest_null_flags,
    int64_t num_rows,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientTableHelper_GetBooleanAsInteropBoolColumn(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t column_index, int8_t *data,
    deephaven::dhcore::interop::InteropBool *optional_dest_null_flags, int64_t num_rows,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientTableHelper_GetStringColumn(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t column_index, deephaven::dhcore::interop::StringHandle *data,
    deephaven::dhcore::interop::InteropBool *optional_dest_null_flags, int64_t num_rows,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_ClientTableHelper_GetDateTimeAsInt64Column(
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> self,
    int32_t column_index, int64_t *data,
    deephaven::dhcore::interop::InteropBool *optional_dest_null_flags, int64_t num_rows,
    deephaven::dhcore::interop::StringPoolHandle *string_pool_handle,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_TickingUpdate_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::dhcore::ticking::TickingUpdate> self);

void deephaven_client_TickingUpdate_Current(
    deephaven::dhcore::interop::NativePtr<deephaven::dhcore::ticking::TickingUpdate> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::interop::ClientTableSpWrapper> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_AggregateCombo_Create(
    const deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *aggregate_ptrs,
    int32_t num_aggregates,
    deephaven::dhcore::interop::NativePtr<deephaven::client::AggregateCombo> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_AggregateCombo_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::AggregateCombo> self);

void deephaven_client_Aggregate_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> self);
void deephaven_client_Aggregate_AbsSum(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Group(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Avg(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Count(
    const char *column,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_First(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Last(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Max(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Med(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Min(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Pct(
    double percentile, deephaven::dhcore::interop::InteropBool avg_median,
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Std(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Sum(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_Var(
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_Aggregate_WAvg(const char *weight,
    const char **columns, int32_t num_columns,
    deephaven::dhcore::interop::NativePtr<deephaven::client::Aggregate> *result,
    deephaven::dhcore::interop::ErrorStatus *status);

void deephaven_client_utility_DurationSpecifier_ctor_nanos(
    int64_t nanos,
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::DurationSpecifier> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_utility_DurationSpecifier_ctor_durationstr(
    const char *duration_str,
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::DurationSpecifier> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_utility_DurationSpecifier_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::DurationSpecifier> result);

void deephaven_client_utility_TimePointSpecifier_ctor_nanos(
    int64_t nanos,
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TimePointSpecifier> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_utility_TimePointSpecifier_ctor_timepointstr(
    const char *time_point_str,
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TimePointSpecifier> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_client_utility_TimePointSpecifier_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TimePointSpecifier> result);

void deephaven_dhclient_utility_TableMaker_ctor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_dtor(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self);
void deephaven_dhclient_utility_TableMaker_MakeTable(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandleManager> manager,
    deephaven::dhcore::interop::NativePtr<deephaven::client::TableHandle> *result,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_AddColumn__CharAsInt16(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    const char *name,
    const int16_t *data,
    int32_t length,
    const deephaven::dhcore::interop::InteropBool *optional_nulls,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_AddColumn__Int8(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    const char *name,
    const int8_t *data,
    int32_t length,
    const deephaven::dhcore::interop::InteropBool *optional_nulls,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_AddColumn__Int16(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    const char *name,
    const int16_t *data,
    int32_t length,
    const deephaven::dhcore::interop::InteropBool *optional_nulls,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_AddColumn__Int32(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    const char *name,
    const int32_t *data,
    int32_t length,
    const deephaven::dhcore::interop::InteropBool *optional_nulls,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_AddColumn__Int64(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    const char *name,
    const int64_t *data,
    int32_t length,
    const deephaven::dhcore::interop::InteropBool *optional_nulls,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_AddColumn__Float(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    const char *name,
    const float *data,
    int32_t length,
    const deephaven::dhcore::interop::InteropBool *optional_nulls,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_AddColumn__Double(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    const char *name,
    const double *data,
    int32_t length,
    const deephaven::dhcore::interop::InteropBool *optional_nulls,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_AddColumn__BoolAsInteropBool(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    const char *name,
    const int8_t *data,
    int32_t length,
    const deephaven::dhcore::interop::InteropBool *optional_nulls,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_AddColumn__String(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    const char *name,
    const char **data,
    int32_t length,
    const deephaven::dhcore::interop::InteropBool *optional_nulls,
    deephaven::dhcore::interop::ErrorStatus *status);
void deephaven_dhclient_utility_TableMaker_AddColumn__DateTimeAsInt64(
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::TableMaker> self,
    const char *name,
    const int64_t *data,
    int32_t length,
    const deephaven::dhcore::interop::InteropBool *optional_nulls,
    deephaven::dhcore::interop::ErrorStatus *status);
}  // extern "C"
