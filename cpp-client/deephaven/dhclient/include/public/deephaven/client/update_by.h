/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#pragma once

#include <string_view>
#include "deephaven/client/client_options.h"
#include "deephaven/client/utility/misc_types.h"
#include "deephaven/dhcore/clienttable/schema.h"
#include "deephaven/dhcore/ticking/ticking.h"

namespace deephaven::client::impl {
class UpdateByOperationImpl;
}  // namespace deephaven::client::impl

namespace deephaven::client {
/**
 * A UpdateByOperation represents an operator for the Table Update-By operation.
 */
class UpdateByOperation {
public:
  /*
   * Default constructor. Creates a (useless) empty client object.
   */
  UpdateByOperation();
  /**
   * Constructor. Used internally.
   */
  explicit UpdateByOperation(std::shared_ptr<impl::UpdateByOperationImpl> impl);
  /**
   * Copy constructor.
   */
  UpdateByOperation(const UpdateByOperation &other);
  /**
   * Copy assignment.
   */
  UpdateByOperation &operator=(const UpdateByOperation &other);
  /**
   * Move constructor
   */
  UpdateByOperation(UpdateByOperation &&other) noexcept;
  /**
   * Move assigment operator.
   */
  UpdateByOperation &operator=(UpdateByOperation &&other) noexcept;
  /**
   * Destructor
   */
  ~UpdateByOperation();

private:
  std::shared_ptr<impl::UpdateByOperationImpl> impl_;

  friend class TableHandle;
};
}  // namespace deephaven::client

namespace deephaven::client::update_by {
/**
 * This enum is meant to be a parallel of the Java enum java.math.MathContext.
 * See https://docs.oracle.com/javase/8/docs/api/java/math/MathContext.html
 */
enum class MathContext : int32_t {
  kUnlimited, kDecimal32, kDecimal64, kDecimal128
};

enum class BadDataBehavior : int32_t {
  kReset, kSkip, kThrow, kPoison
};

enum class DeltaControl : int32_t {
  kNullDominates, kValueDominates, kZeroDominates
};

struct OperationControl {
  explicit OperationControl(BadDataBehavior on_null = BadDataBehavior::kSkip,
      BadDataBehavior on_na_n = BadDataBehavior::kSkip,
      MathContext big_value_context = MathContext::kDecimal128) : on_null(on_null),
      on_nan(on_na_n),big_value_context(big_value_context) {}

  BadDataBehavior on_null;
  BadDataBehavior on_nan;
  MathContext big_value_context;
};

/**
 * Creates a cumulative sum UpdateByOperation for the supplied column names.
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *  i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 */
UpdateByOperation cumSum(std::vector<std::string> cols);
/**
 * Creates a cumulative product UpdateByOperation for the supplied column names.
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *  i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 */
UpdateByOperation cumProd(std::vector<std::string> cols);
/**
 * Creates a cumulative minimum UpdateByOperation for the supplied column names.
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *  i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 */
UpdateByOperation cumMin(std::vector<std::string> cols);
/**
 * Creates a cumulative maximum UpdateByOperation for the supplied column names.
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *  i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 */
UpdateByOperation cumMax(std::vector<std::string> cols);
/**
 * Creates a forward fill UpdateByOperation for the supplied column names.
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *  i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 */
UpdateByOperation forwardFill(std::vector<std::string> cols);
/**
 * Creates a delta UpdateByOperation for the supplied column names. The Delta operation produces values By computing
 * the difference between the current value and the prev1G/ious value. When the current value is null, this operation
 * will output null. When the current value is valid, the output will depend on the DeltaControl provided.
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param deltaControl
 *   When not provided or set to NULL_DOMINATES, a value following a null value returns null.
 *   When set to VALUE_DOMINATES, a value following a null value returns the value.
 *   When set to ZERO_DOMINATES, a value following a null value returns zero.
 */
UpdateByOperation delta(std::vector<std::string> cols, DeltaControl delta_control = DeltaControl::kNullDominates);
/**
 * Creates an EMA (exponential moving average) UpdateByOperation for the supplied column names,
 * using ticks as the decay unit.
 * The formula used is
 *   a = e^(-1 / decay_ticks)
 *   ema_next = a * ema_last + (1 - a) * value
 * @param decayTicks the decay rate in ticks
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param opControl defines how special cases should behave
 */
UpdateByOperation emaTick(double decay_ticks, std::vector<std::string> cols,
    const OperationControl &op_control = OperationControl());
/**
 * Creates an EMA (exponential moving average) UpdateByOperation for the supplied column names,
 * using time as the decay unit.
 * The formula used is
 *   a = e^(-dt / decay_time)
 *   ema_next = a * ema_last + (1 - a) * value
 *
 * @param timestampCol the column in the source table to use for timestamps
 * @param decayTime the decay rate, specified as a std::chrono::duration or as an ISO 8601 duration
 *   string
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param opControl defines how special cases should behave
 */
UpdateByOperation emaTime(std::string timestamp_col,
    deephaven::client::utility::DurationSpecifier decay_time, std::vector<std::string> cols,
    const OperationControl &op_control = OperationControl());
/**
 * Creates an EMS (exponential moving sum) UpdateByOperation for the supplied column names, using
 * ticks as the decay unit.
 * The formula used is
 *   a = e^(-1 / decay_ticks)
 *   ems_next = a * ems_last + value
 * @param decayTicks the decay rate in ticks
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param opControl defines how special cases should behave
 */
UpdateByOperation emsTick(double decay_ticks, std::vector<std::string> cols,
    const OperationControl &op_control = OperationControl());
/**
 * Creates an EMS (exponential moving sum) UpdateByOperation for the supplied column names, using
 * time as the decay unit.
 * The formula used is
 *   a = e^(-dt / decay_time)
 *   eems_next = a * ems_last + value
 *
 * @param timestampCol the column in the source table to use for timestamps
 * @param decayTime the decay rate, specified as a std::chrono::duration or as an ISO 8601 duration
 *   string
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param opControl defines how special cases should behave
 */
UpdateByOperation emsTime(std::string timestamp_col,
    deephaven::client::utility::DurationSpecifier decay_time, std::vector<std::string> cols,
    const OperationControl &op_control = OperationControl());
/**
 * Creates an EM Min (exponential moving minimum) UpdateByOperation for the supplied column names,
 * using ticks as the decay unit.
 * The formula used is
 *   a = e^(-1 / decay_ticks)
 *   em_val_next = Min(a * em_val_last, value)
 * @param decayTicks the decay rate in ticks
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param opControl defines how special cases should behave
 */
UpdateByOperation emminTick(double decay_ticks, std::vector<std::string> cols,
    const OperationControl &op_control = OperationControl());
/**
 * Creates an EM Min (exponential moving minimum) UpdateByOperation for the supplied column names,
 * using time as the decay unit.
 * The formula used is
 *   a = e^(-dt / decay_time)
 *   em_val_next = Min(a * em_val_last, value)
 * @param timestampCol the column in the source table to use for timestamps
 * @param decayTime the decay rate, specified as a std::chrono::duration or as an ISO 8601 duration
 *   string
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param opControl defines how special cases should behave
 */
UpdateByOperation emminTime(std::string timestamp_col,
    deephaven::client::utility::DurationSpecifier decay_time, std::vector<std::string> cols,
    const OperationControl &op_control = OperationControl());
/**
 * Creates an EM Max (exponential moving maximum) UpdateByOperation for the supplied column names,
 * using ticks as the decay unit.
 * The formula used is
 *   a = e^(-1 / decay_ticks)
 *   em_val_next = Max(a * em_val_last, value)
 * @param decayTicks the decay rate in ticks
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param opControl defines how special cases should behave
 */
UpdateByOperation emmaxTick(double decay_ticks, std::vector<std::string> cols,
    const OperationControl &op_control = OperationControl());
/**
 * Creates an EM Max (exponential moving maximum) UpdateByOperation for the supplied column names,
 * using time as the decay unit.
 * The formula used is
 *   a = e^(-dt / decay_time)
 *   em_val_next = Max(a * em_val_last, value)
 * @param timestampCol the column in the source table to use for timestamps
 * @param decayTime the decay rate, specified as a std::chrono::duration or as an ISO 8601 duration
 *   string
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param opControl defines how special cases should behave
 */
UpdateByOperation emmaxTime(std::string timestamp_col,
    deephaven::client::utility::DurationSpecifier decay_time, std::vector<std::string> cols,
    const OperationControl &op_control = OperationControl());
/**
 * Creates an EM Std (exponential moving standard deviation) UpdateByOperation for the supplied
 * column names, using ticks as the decay unit.
 * The formula used is
 *   a = e^(-1 / decay_ticks)
 *   variance = a * (prevVariance + (1 − a) * (x − prevEma)^2)
 *   ema = a * prevEma + x
 *   std = sqrt(variance)
 * @param decayTicks the decay rate in ticks
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param opControl defines how special cases should behave
 */
UpdateByOperation emstdTick(double decay_ticks, std::vector<std::string> cols,
    const OperationControl &op_control = OperationControl());
/**
 * Creates an EM Std (exponential moving standard deviation) UpdateByOperation for the supplied
 * column names, using time as the decay unit.
 * The formula used is
 *   a = e^(-dt / timeDecay)
 *   variance = a * (prevVariance + (1 − a) * (x − prevEma)^2)
 *   ema = a * prevEma + x
 *   std = sqrt(variance)
 * @param timestampCol the column in the source table to use for timestamps
 * @param decayTime the decay rate, specified as a std::chrono::duration or as an ISO 8601 duration
 *   string
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param opControl defines how special cases should behave
 */
UpdateByOperation emstdTime(std::string timestamp_col,
    deephaven::client::utility::DurationSpecifier decay_time, std::vector<std::string> cols,
    const OperationControl &op_control = OperationControl());
/**
 * Creates a rolling sum UpdateByOperation for the supplied column names, using ticks as the
 * windowing unit. Ticks are row counts, and you may specify the reverse and forward window in
 * number of rows to include. The current row is considered to belong to the reverse window but
 * not the forward window. Also, negative values are allowed and can be used to generate completely
 * forward or completely reverse windows.
 *
 * Here are some examples of window values:
 *   rev_ticks = 1, fwd_ticks = 0 - contains only the current row
 *   rev_ticks = 10, fwd_ticks = 0 - contains 9 previous rows and the current row
 *   rev_ticks = 0, fwd_ticks = 10 - contains the following 10 rows, excludes the current row
 *   rev_ticks = 10, fwd_ticks = 10 - contains the previous 9 rows, the current row and the 10 rows
 *     following
 *   rev_ticks = 10, fwd_ticks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows
 *     before the current row (inclusive)
 *   rev_ticks = 11, fwd_ticks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row
 *     before the current row (inclusive)
 *    rev_ticks = -5, fwd_ticks = 10 - contains 5 rows, beginning 5 rows following, ending at 10
 *      rows following the current row (inclusive)
 *
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTicks the look-behind window size (in rows/ticks)
 * @param fwdTicks the look-forward window size (int rows/ticks), default is 0
 */
UpdateByOperation rollingSumTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks = 0);
/**
 * Creates a rolling sum UpdateByOperation for the supplied column names, using time as the
 * windowing unit. This function accepts nanoseconds or time strings as the reverse and forward
 * window parameters. Negative values are allowed and can be used to generate completely forward or
 * completely reverse windows. A row containing a null in the timestamp column belongs to no window
 * and will not be considered in the windows of other rows; its output will be null.
 * Here are some examples of window values:
 *   rev_time = 0, fwd_time = 0 - contains rows that exactly match the current row timestamp
 *   rev_time = "PT00:10:00", fwd_time = "0" - contains rows from 10m before through the current row
 *     timestamp (inclusive)
 *   rev_time = 0, fwd_time = 600_000_000_000 - contains rows from the current row through 10m
 *     following the current row timestamp (inclusive)
 *   rev_time = "PT00:10:00", fwd_time = "PT00:10:00" - contains rows from 10m before through 10m
 *     following the current row timestamp (inclusive)
 *   rev_time = "PT00:10:00", fwd_time = "-PT00:05:00" - contains rows from 10m before through 5m
 *     before the current row timestamp (inclusive), this is a purely backwards looking window
 *   rev_time = "-PT00:05:00", fwd_time = "PT00:10:00"} - contains rows from 5m following through
 *     10m following the current row timestamp (inclusive), this is a purely forwards looking window
 * @param timestampCol the timestamp column for determining the window
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTime the look-behind window size
 * @param fwdTime the look-ahead window size
 */
UpdateByOperation rollingSumTime(std::string timestamp_col, std::vector<std::string> cols,
    deephaven::client::utility::DurationSpecifier rev_time,
    deephaven::client::utility::DurationSpecifier fwd_time = 0);
/**
 * Creates a rolling Group UpdateByOperation for the supplied column names, using ticks as the
 * windowing unit. Ticks are row counts, and you may specify the reverse and forward window in
 * number of rows to include. The current row is considered to belong to the reverse window but
 * not the forward window. Also, negative values are allowed and can be used to generate completely
 * forward or completely reverse windows.
 *
 * See the documentation of rollingSumTick() for examples of window values.
 *
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTicks the look-behind window size (in rows/ticks)
 * @param fwdTicks the look-forward window size (int rows/ticks), default is 0
 */
UpdateByOperation rollingGroupTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks = 0);
/**
 * Creates a rolling Group UpdateByOperation for the supplied column names, using time as the
 * windowing unit. This function accepts nanoseconds or time strings as the reverse and forward
 * window parameters. Negative values are allowed and can be used to generate completely forward or
 * completely reverse windows. A row containing a null in the timestamp column belongs to no window
 * and will not be considered in the windows of other rows; its output will be null.
 *
 * See the documentation of rollingSumTime() for examples of window values.
 *
 * @param timestampCol the timestamp column for determining the window
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTime the look-behind window size
 * @param fwdTime the look-ahead window size
 */
UpdateByOperation rollingGroupTime(std::string timestamp_col, std::vector<std::string> cols,
    deephaven::client::utility::DurationSpecifier rev_time,
    deephaven::client::utility::DurationSpecifier fwd_time = 0);
/**
 * Creates a rolling average UpdateByOperation for the supplied column names, using ticks as the
 * windowing unit. Ticks are row counts, and you may specify the reverse and forward window in
 * number of rows to include. The current row is considered to belong to the reverse window but
 * not the forward window. Also, negative values are allowed and can be used to generate completely
 * forward or completely reverse windows.
 *
 * See the documentation of rollingSumTick() for examples of window values.
 *
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTicks the look-behind window size (in rows/ticks)
 * @param fwdTicks the look-forward window size (int rows/ticks), default is 0
 */
UpdateByOperation rollingAvgTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks = 0);
/**
 * Creates a rolling average UpdateByOperation for the supplied column names, using time as the
 * windowing unit. This function accepts nanoseconds or time strings as the reverse and forward
 * window parameters. Negative values are allowed and can be used to generate completely forward or
 * completely reverse windows. A row containing a null in the timestamp column belongs to no window
 * and will not be considered in the windows of other rows; its output will be null.
 *
 * See the documentation of rollingSumTime() for examples of window values.
 *
 * @param timestampCol the timestamp column for determining the window
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTime the look-behind window size
 * @param fwdTime the look-ahead window size
 */
UpdateByOperation rollingAvgTime(std::string timestamp_col, std::vector<std::string> cols,
    deephaven::client::utility::DurationSpecifier rev_time,
    deephaven::client::utility::DurationSpecifier fwd_time = 0);
/**
 * Creates a rolling Min UpdateByOperation for the supplied column names, using ticks as the
 * windowing unit. Ticks are row counts, and you may specify the reverse and forward window in
 * number of rows to include. The current row is considered to belong to the reverse window but
 * not the forward window. Also, negative values are allowed and can be used to generate completely
 * forward or completely reverse windows.
 *
 * See the documentation of rollingSumTick() for examples of window values.
 *
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTicks the look-behind window size (in rows/ticks)
 * @param fwdTicks the look-forward window size (int rows/ticks), default is 0
 */
UpdateByOperation rollingMinTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks = 0);
/**
 * Creates a rolling Min UpdateByOperation for the supplied column names, using time as the
 * windowing unit. This function accepts nanoseconds or time strings as the reverse and forward
 * window parameters. Negative values are allowed and can be used to generate completely forward or
 * completely reverse windows. A row containing a null in the timestamp column belongs to no window
 * and will not be considered in the windows of other rows; its output will be null.
 *
 * See the documentation of rollingSumTime() for examples of window values.
 *
 * @param timestampCol the timestamp column for determining the window
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTime the look-behind window size
 * @param fwdTime the look-ahead window size
 */
UpdateByOperation rollingMinTime(std::string timestamp_col, std::vector<std::string> cols,
    deephaven::client::utility::DurationSpecifier rev_time,
    deephaven::client::utility::DurationSpecifier fwd_time = 0);
/**
 * Creates a rolling Max UpdateByOperation for the supplied column names, using ticks as the
 * windowing unit. Ticks are row counts, and you may specify the reverse and forward window in
 * number of rows to include. The current row is considered to belong to the reverse window but
 * not the forward window. Also, negative values are allowed and can be used to generate completely
 * forward or completely reverse windows.
 *
 * See the documentation of rollingSumTick() for examples of window values.
 *
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTicks the look-behind window size (in rows/ticks)
 * @param fwdTicks the look-forward window size (int rows/ticks), default is 0
 */
UpdateByOperation rollingMaxTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks = 0);
/**
 * Creates a rolling Max UpdateByOperation for the supplied column names, using time as the
 * windowing unit. This function accepts nanoseconds or time strings as the reverse and forward
 * window parameters. Negative values are allowed and can be used to generate completely forward or
 * completely reverse windows. A row containing a null in the timestamp column belongs to no window
 * and will not be considered in the windows of other rows; its output will be null.
 *
 * See the documentation of rollingSumTime() for examples of window values.
 *
 * @param timestampCol the timestamp column for determining the window
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTime the look-behind window size
 * @param fwdTime the look-ahead window size
 */
UpdateByOperation rollingMaxTime(std::string timestamp_col, std::vector<std::string> cols,
    deephaven::client::utility::DurationSpecifier rev_time,
    deephaven::client::utility::DurationSpecifier fwd_time = 0);
/**
 * Creates a rolling product UpdateByOperation for the supplied column names, using ticks as the
 * windowing unit. Ticks are row counts, and you may specify the reverse and forward window in
 * number of rows to include. The current row is considered to belong to the reverse window but
 * not the forward window. Also, negative values are allowed and can be used to generate completely
 * forward or completely reverse windows.
 *
 * See the documentation of rollingSumTick() for examples of window values.
 *
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTicks the look-behind window size (in rows/ticks)
 * @param fwdTicks the look-forward window size (int rows/ticks), default is 0
 */
UpdateByOperation rollingProdTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks = 0);
/**
 * Creates a rolling product UpdateByOperation for the supplied column names, using time as the
 * windowing unit. This function accepts nanoseconds or time strings as the reverse and forward
 * window parameters. Negative values are allowed and can be used to generate completely forward or
 * completely reverse windows. A row containing a null in the timestamp column belongs to no window
 * and will not be considered in the windows of other rows; its output will be null.
 *
 * See the documentation of rollingSumTime() for examples of window values.
 *
 * @param timestampCol the timestamp column for determining the window
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTime the look-behind window size
 * @param fwdTime the look-ahead window size
 */
UpdateByOperation rollingProdTime(std::string timestamp_col, std::vector<std::string> cols,
    deephaven::client::utility::DurationSpecifier rev_time,
    deephaven::client::utility::DurationSpecifier fwd_time = 0);
/**
 * Creates a rolling count UpdateByOperation for the supplied column names, using ticks as the
 * windowing unit. Ticks are row counts, and you may specify the reverse and forward window in
 * number of rows to include. The current row is considered to belong to the reverse window but
 * not the forward window. Also, negative values are allowed and can be used to generate completely
 * forward or completely reverse windows.
 *
 * See the documentation of rollingSumTick() for examples of window values.
 *
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTicks the look-behind window size (in rows/ticks)
 * @param fwdTicks the look-forward window size (int rows/ticks), default is 0
 */
UpdateByOperation rollingCountTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks = 0);
/**
 * Creates a rolling count UpdateByOperation for the supplied column names, using time as the
 * windowing unit. This function accepts nanoseconds or time strings as the reverse and forward
 * window parameters. Negative values are allowed and can be used to generate completely forward or
 * completely reverse windows. A row containing a null in the timestamp column belongs to no window
 * and will not be considered in the windows of other rows; its output will be null.
 *
 * See the documentation of rollingSumTime() for examples of window values.
 *
 * @param timestampCol the timestamp column for determining the window
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTime the look-behind window size
 * @param fwdTime the look-ahead window size
 */
UpdateByOperation rollingCountTime(std::string timestamp_col, std::vector<std::string> cols,
    deephaven::client::utility::DurationSpecifier rev_time,
    deephaven::client::utility::DurationSpecifier fwd_time = 0);
/**
 * Creates a rolling sample standard deviation UpdateByOperation for the supplied column names, using ticks as the
 * windowing unit. Ticks are row counts, and you may specify the reverse and forward window in number of rows to include.
 * The current row is considered to belong to the reverse window but not the forward window. Also, negative values are
 * allowed and can be used to generate completely forward or completely reverse windows.
 *
 * Sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
 * which ensures that the sample variance will be an unbiased estimator of population variance.
 *
 * See the documentation of rollingSumTick() for examples of window values.
 *
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTicks the look-behind window size (in rows/ticks)
 * @param fwdTicks the look-forward window size (int rows/ticks), default is 0
 */
UpdateByOperation rollingStdTick(std::vector<std::string> cols, int rev_ticks, int fwd_ticks = 0);
/**
 * Creates a rolling sample standard deviation UpdateByOperation for the supplied column names, using time as the
 * windowing unit. This function accepts nanoseconds or time strings as the reverse and forward window parameters.
 * Negative values are allowed and can be used to generate completely forward or completely reverse windows.
 * A row containing a null in the timestamp column belongs to no window and will not be considered in the windows
 * of other rows; its output will be null.
 *
 * Sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
 * which ensures that the sample variance will be an unbiased estimator of population variance.
 *
 * See the documentation of rollingSumTime() for examples of window values.
 *
 * @param timestampCol the timestamp column for determining the window
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTime the look-behind window size
 * @param fwdTime the look-ahead window size
 */
UpdateByOperation rollingStdTime(std::string timestamp_col, std::vector<std::string> cols,
    deephaven::client::utility::DurationSpecifier rev_time,
    deephaven::client::utility::DurationSpecifier fwd_time = 0);
/**
 * Creates a rolling weighted average UpdateByOperation for the supplied column names, using ticks as the
 * windowing unit. Ticks are row counts, and you may specify the reverse and forward window in
 * number of rows to include. The current row is considered to belong to the reverse window but
 * not the forward window. Also, negative values are allowed and can be used to generate completely
 * forward or completely reverse windows.
 *
 * See the documentation of rollingSumTick() for examples of window values.
 *
 * @param weightCol the column containing the weight values
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTicks the look-behind window size (in rows/ticks)
 * @param fwdTicks the look-forward window size (int rows/ticks), default is 0
 */
UpdateByOperation rollingWavgTick(std::string weight_col, std::vector<std::string> cols,
    int rev_ticks, int fwd_ticks = 0);
/**
 * Creates a rolling weighted average UpdateByOperation for the supplied column names, using time as the
 * windowing unit. This function accepts nanoseconds or time strings as the reverse and forward
 * window parameters. Negative values are allowed and can be used to generate completely forward or
 * completely reverse windows. A row containing a null in the timestamp column belongs to no window
 * and will not be considered in the windows of other rows; its output will be null.
 *
 * See the documentation of rollingSumTime() for examples of window values.
 *
 * @param weightCol the column containing the weight values
 * @param timestampCol the timestamp column for determining the window
 * @param cols the column(s) to be operated on, can include expressions to rename the output,
 *   i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
 * @param revTime the look-behind window size
 * @param fwdTime the look-ahead window size
 */
UpdateByOperation rollingWavgTime(std::string timestamp_col, std::string weight_col,
    std::vector<std::string> cols, deephaven::client::utility::DurationSpecifier rev_time,
    deephaven::client::utility::DurationSpecifier fwd_time = 0);
}  // namespace deephaven::client::update_by
