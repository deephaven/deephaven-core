/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/client/interop/update_by_interop.h"

#include "deephaven/client/client.h"
#include "deephaven/client/client_options.h"
#include "deephaven/client/update_by.h"
#include "deephaven/dhcore/interop/interop_util.h"

using deephaven::dhcore::interop::ErrorStatus;
using deephaven::dhcore::interop::InteropBool;
using deephaven::dhcore::interop::NativePtr;

using deephaven::client::ClientOptions;
using deephaven::client::UpdateByOperation;
using deephaven::client::update_by::DeltaControl;
using deephaven::client::update_by::OperationControl;
using deephaven::client::utility::DurationSpecifier;

namespace {
void WithColsHelper(const char **cols, int32_t num_cols,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status,
    UpdateByOperation (*fp)(std::vector<std::string>)) {
  status->Run([=]() {
    std::vector<std::string> columns(cols, cols + num_cols);
    auto ubo = (*fp)(std::move(columns));
    result->Reset(new UpdateByOperation(std::move(ubo)));
  });
}

void WithTicksHelper(double decay_ticks,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status,
    UpdateByOperation (*fp)(double, std::vector<std::string>, const OperationControl &)) {
  status->Run([=]() {
    std::vector<std::string> columns(cols, cols + num_cols);
    auto ubo = (*fp)(decay_ticks, std::move(columns), *op_control);
    result->Reset(new UpdateByOperation(std::move(ubo)));
  });
}

void WithTimeHelper(const char *timestamp_col,
    NativePtr<DurationSpecifier> decay_time,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status,
    UpdateByOperation (*fp)(std::string, DurationSpecifier, std::vector<std::string>, const OperationControl &)) {
  status->Run([=]() {
    std::vector<std::string> columns(cols, cols + num_cols);
    auto ubo = (*fp)(timestamp_col, *decay_time, std::move(columns), *op_control);
    result->Reset(new UpdateByOperation(std::move(ubo)));
  });
}

void WithRollingTicksHelper(
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status,
    UpdateByOperation (*fp)(std::vector<std::string>, int32_t, int32_t)) {
  status->Run([=]() {
    std::vector<std::string> columns(cols, cols + num_cols);
    auto ubo = (*fp)(std::move(columns), rev_ticks, fwd_ticks);
    result->Reset(new UpdateByOperation(std::move(ubo)));
  });
}

void WithRollingTimeHelper(const char *timestamp_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status,
    UpdateByOperation (*fp)(std::string, std::vector<std::string>, DurationSpecifier, DurationSpecifier)) {
  status->Run([=]() {
    std::vector<std::string> columns(cols, cols + num_cols);
    auto ubo = (*fp)(timestamp_col, std::move(columns), *rev_time, *fwd_time);
    result->Reset(new UpdateByOperation(std::move(ubo)));
  });
}

void WithRollingWavgTicksHelper(const char *weight_col,
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status,
    UpdateByOperation (*fp)(std::string, std::vector<std::string>, int32_t, int32_t)) {
  status->Run([=]() {
    std::vector<std::string> columns(cols, cols + num_cols);
    auto ubo = (*fp)(weight_col, std::move(columns), rev_ticks, fwd_ticks);
    result->Reset(new UpdateByOperation(std::move(ubo)));
  });
}

void WithRollingWavgTimeHelper(const char *timestamp_col, const char *weight_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status,
    UpdateByOperation (*fp)(std::string, std::string, std::vector<std::string>, DurationSpecifier, DurationSpecifier)) {
  status->Run([=]() {
    std::vector<std::string> columns(cols, cols + num_cols);
    auto ubo = (*fp)(timestamp_col, weight_col, std::move(columns), *rev_time, *fwd_time);
    result->Reset(new UpdateByOperation(std::move(ubo)));
  });
}
}  // namespace

extern "C" {
void deephaven_client_UpdateByOperation_dtor(
    NativePtr<UpdateByOperation> self) {
  delete self.Get();
}

void deephaven_client_update_by_cumSum(
    const char **cols, int32_t num_cols,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithColsHelper(cols, num_cols, result, status, &deephaven::client::update_by::cumSum);
}

void deephaven_client_update_by_cumProd(
    const char **cols, int32_t num_cols,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithColsHelper(cols, num_cols, result, status, &deephaven::client::update_by::cumProd);
}

void deephaven_client_update_by_cumMin(
    const char **cols, int32_t num_cols,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithColsHelper(cols, num_cols, result, status, &deephaven::client::update_by::cumMin);
}

void deephaven_client_update_by_cumMax(
    const char **cols, int32_t num_cols,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithColsHelper(cols, num_cols, result, status, &deephaven::client::update_by::cumMax);
}

void deephaven_client_update_by_forwardFill(
    const char **cols, int32_t num_cols,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithColsHelper(cols, num_cols, result, status, &deephaven::client::update_by::forwardFill);
}

void deephaven_client_update_by_delta(
    const char **cols, int32_t num_cols,
    deephaven::client::update_by::DeltaControl delta_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  status->Run([=]() {
    std::vector<std::string> columns(cols, cols + num_cols);
    auto ubo = deephaven::client::update_by::delta(std::move(columns), delta_control);
    result->Reset(new UpdateByOperation(std::move(ubo)));
  });
}

void deephaven_client_update_by_emaTick(double decay_ticks,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithTicksHelper(decay_ticks, cols, num_cols, op_control, result, status,
      &deephaven::client::update_by::emaTick);
}

void deephaven_client_update_by_emaTime(const char *timestamp_col,
    NativePtr<DurationSpecifier> decay_time,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithTimeHelper(timestamp_col, decay_time, cols, num_cols, op_control, result, status,
      &deephaven::client::update_by::emaTime);
}

void deephaven_client_update_by_emsTick(double decay_ticks,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithTicksHelper(decay_ticks, cols, num_cols, op_control, result, status,
      &deephaven::client::update_by::emsTick);
}

void deephaven_client_update_by_emsTime(const char *timestamp_col,
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::DurationSpecifier> decay_time,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithTimeHelper(timestamp_col, decay_time, cols, num_cols, op_control, result, status,
      &deephaven::client::update_by::emsTime);
}

void deephaven_client_update_by_emminTick(double decay_ticks,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithTicksHelper(decay_ticks, cols, num_cols, op_control, result, status,
      &deephaven::client::update_by::emminTick);
}

void deephaven_client_update_by_emminTime(const char *timestamp_col,
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::DurationSpecifier> decay_time,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithTimeHelper(timestamp_col, decay_time, cols, num_cols, op_control, result, status,
      &deephaven::client::update_by::emminTime);
}

void deephaven_client_update_by_emmaxTick(double decay_ticks,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithTicksHelper(decay_ticks, cols, num_cols, op_control, result, status,
      &deephaven::client::update_by::emmaxTick);
}

void deephaven_client_update_by_emmaxTime(const char *timestamp_col,
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::DurationSpecifier> decay_time,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithTimeHelper(timestamp_col, decay_time, cols, num_cols, op_control, result, status,
      &deephaven::client::update_by::emmaxTime);
}

void deephaven_client_update_by_emstdTick(double decay_ticks,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithTicksHelper(decay_ticks, cols, num_cols, op_control, result, status,
      &deephaven::client::update_by::emstdTick);
}

void deephaven_client_update_by_emstdTime(const char *timestamp_col,
    deephaven::dhcore::interop::NativePtr<deephaven::client::utility::DurationSpecifier> decay_time,
    const char **cols, int32_t num_cols,
    const OperationControl *op_control,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithTimeHelper(timestamp_col, decay_time, cols, num_cols, op_control, result, status,
      &deephaven::client::update_by::emstdTime);
}

void deephaven_client_update_by_rollingSumTick(
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTicksHelper(cols, num_cols, rev_ticks, fwd_ticks, result, status,
      &deephaven::client::update_by::rollingSumTick);
}

void deephaven_client_update_by_rollingSumTime(const char *timestamp_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTimeHelper(timestamp_col, cols, num_cols, rev_time, fwd_time, result, status,
      &deephaven::client::update_by::rollingSumTime);
}

void deephaven_client_update_by_rollingGroupTick(
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTicksHelper(cols, num_cols, rev_ticks, fwd_ticks, result, status,
      &deephaven::client::update_by::rollingGroupTick);
}

void deephaven_client_update_by_rollingGroupTime(const char *timestamp_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTimeHelper(timestamp_col, cols, num_cols, rev_time, fwd_time, result, status,
      &deephaven::client::update_by::rollingGroupTime);
}

void deephaven_client_update_by_rollingAvgTick(
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTicksHelper(cols, num_cols, rev_ticks, fwd_ticks, result, status,
      &deephaven::client::update_by::rollingAvgTick);
}

void deephaven_client_update_by_rollingAvgTime(const char *timestamp_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTimeHelper(timestamp_col, cols, num_cols, rev_time, fwd_time, result, status,
      &deephaven::client::update_by::rollingAvgTime);
}

void deephaven_client_update_by_rollingMinTick(
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTicksHelper(cols, num_cols, rev_ticks, fwd_ticks, result, status,
      &deephaven::client::update_by::rollingMinTick);
}

void deephaven_client_update_by_rollingMinTime(const char *timestamp_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTimeHelper(timestamp_col, cols, num_cols, rev_time, fwd_time, result, status,
      &deephaven::client::update_by::rollingMinTime);
}

void deephaven_client_update_by_rollingMaxTick(
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTicksHelper(cols, num_cols, rev_ticks, fwd_ticks, result, status,
      &deephaven::client::update_by::rollingMaxTick);
}

void deephaven_client_update_by_rollingMaxTime(const char *timestamp_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTimeHelper(timestamp_col, cols, num_cols, rev_time, fwd_time, result, status,
      &deephaven::client::update_by::rollingMaxTime);
}

void deephaven_client_update_by_rollingProdTick(
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTicksHelper(cols, num_cols, rev_ticks, fwd_ticks, result, status,
      &deephaven::client::update_by::rollingProdTick);
}

void deephaven_client_update_by_rollingProdTime(const char *timestamp_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTimeHelper(timestamp_col, cols, num_cols, rev_time, fwd_time, result, status,
      &deephaven::client::update_by::rollingProdTime);
}

void deephaven_client_update_by_rollingCountTick(
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTicksHelper(cols, num_cols, rev_ticks, fwd_ticks, result, status,
      &deephaven::client::update_by::rollingCountTick);
}

void deephaven_client_update_by_rollingCountTime(const char *timestamp_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTimeHelper(timestamp_col, cols, num_cols, rev_time, fwd_time, result, status,
      &deephaven::client::update_by::rollingCountTime);
}

void deephaven_client_update_by_rollingStdTick(
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTicksHelper(cols, num_cols, rev_ticks, fwd_ticks, result, status,
      &deephaven::client::update_by::rollingStdTick);
}

void deephaven_client_update_by_rollingStdTime(const char *timestamp_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingTimeHelper(timestamp_col, cols, num_cols, rev_time, fwd_time, result, status,
      &deephaven::client::update_by::rollingStdTime);
}

void deephaven_client_update_by_rollingWavgTick(const char *weight_col,
    const char **cols, int32_t num_cols,
    int32_t rev_ticks, int32_t fwd_ticks,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingWavgTicksHelper(weight_col, cols, num_cols, rev_ticks, fwd_ticks, result, status,
      &deephaven::client::update_by::rollingWavgTick);
}

void deephaven_client_update_by_rollingWavgTime(const char *timestamp_col,
    const char *weight_col,
    const char **cols, int32_t num_cols,
    NativePtr<DurationSpecifier> rev_time,
    NativePtr<DurationSpecifier> fwd_time,
    NativePtr<UpdateByOperation> *result,
    ErrorStatus *status) {
  WithRollingWavgTimeHelper(timestamp_col, weight_col, cols, num_cols, rev_time, fwd_time,
      result, status,
      &deephaven::client::update_by::rollingWavgTime);
}
}  // extern "C"
