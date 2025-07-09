//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Io.Deephaven.Proto.Backplane.Grpc;
using UpdateByOperationProto = Io.Deephaven.Proto.Backplane.Grpc.UpdateByRequest.Types.UpdateByOperation;
using BadDataBehaviorProtoEnum = Io.Deephaven.Proto.Backplane.Grpc.BadDataBehavior;
using MathContextProto = Io.Deephaven.Proto.Backplane.Grpc.MathContext;
using RoundingModeProtoEnum = Io.Deephaven.Proto.Backplane.Grpc.MathContext.Types.RoundingMode;
using Google.Protobuf.WellKnownTypes;

namespace Deephaven.Dh_NetClient;

/// <summary>
/// An Enum for predefined precision and rounding settings in numeric calculations.
/// </summary>
public enum MathContext : Int32 {
  /// <summary>
  /// unlimited precision arithmetic, rounding is half-up
  /// </summary>
  Unlimited,
  /// <summary>
  ///  a precision setting matching the IEEE 754R Decimal32 format, 7 digits, rounding is half-even
  /// </summary>
  Decimal32,
  /// <summary>
  /// A precision setting matching the IEEE 754R Decimal64 format, 16 digits, rounding is half-even
  /// </summary>
  Decimal64,
  /// <summary>
  /// A precision setting matching the IEEE 754R Decimal128 format, 34 digits, rounding is half-even
  /// </summary>
  Decimal128
}

/// <summary>
/// An Enum defining ways to handle invalid data during update-by operations.
/// </summary>
public enum BadDataBehavior : Int32 {
  /// <summary>
  /// Reset the state for the bucket to None when invalid data is encountered
  /// </summary>
  Reset,
  /// <summary>
  /// Skip and do not process the invalid data without changing state
  /// </summary>
  Skip,
  /// <summary>
  /// Throw an exception and abort processing when bad data is encountered
  /// </summary>
  Throw,
  /// <summary>
  /// Allow the bad data to poison the result. This is only valid for use with NaN
  /// </summary>
  Poison
}

/// <summary>
/// An Enum defining ways to handle null values during update-by Delta operations where delta operations
/// return the difference between the current row and the previous row.
/// </summary>
public enum DeltaControl : Int32 {
  /// <summary>
  /// A valid value following a null value returns null.
  /// </summary>
  NullDominates,
  /// <summary>
  /// A valid value following a null value returns the valid value.
  /// </summary>
  ValueDominates,
  /// <summary>
  /// A valid value following a null value returns zero.
  /// </summary>
  ZeroDominates
}

/// <summary>
/// Represents control parameters for performing operations with the table
/// UpdateByOperation.
/// </summary>
/// <param name="OnNull">the behavior for when null values are encountered, default is SKIP</param>
/// <param name="OnNan">the behavior for when NaN values are encountered, default is SKIP</param>
/// <param name="BigValueContext">the context to use when processing arbitrary precision numeric values,
/// default is DECIMAL128.</param>
public record OperationControl(
  BadDataBehavior OnNull = BadDataBehavior.Skip,
  BadDataBehavior OnNan = BadDataBehavior.Skip,
  MathContext BigValueContext = MathContext.Decimal128);

public class UpdateByOperation {
  public readonly UpdateByOperationProto UpdateByProto;

  public UpdateByOperation(UpdateByRequest.Types.UpdateByOperation updateByProto) {
    UpdateByProto = updateByProto;
  }

  /// <summary>
  /// Creates a cumulative sum UpdateByOperation for the supplied column names.
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
  ///</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation CumSum(params string[] cols) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Sum =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByCumulativeSum();
    return ubb.Build();
  }

  /// <summary>
  /// Creates a cumulative product UpdateByOperation for the supplied column names.
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
  ///</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation CumProd(params string[] cols) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Product = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByCumulativeProduct();
    return ubb.Build();
  }

  /// <summary>
  /// Creates a cumulative minimum UpdateByOperation for the supplied column names.
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
  ///</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation CumMin(params string[] cols) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Min = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByCumulativeMin();
    return ubb.Build();
  }

  /// <summary>
  /// Creates a cumulative maximum UpdateByOperation for the supplied column names.
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
  ///</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation CumMax(params string[] cols) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Max = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByCumulativeMax();
    return ubb.Build();
  }

  /// <summary>
  /// Creates a forward fill UpdateByOperation for the supplied column names. Null values in the
  /// column(s) are replaced by the last known non-null values. This operation is forward only.
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
  ///</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation ForwardFill(params string[] cols) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Fill = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByFill();
    return ubb.Build();
  }

  /// <summary>
  /// Creates a delta UpdateByOperation for the supplied column names. The Delta operation produces
  /// values by computing the difference between the current value and the previous value. When the
  /// current value is null, this operation will output null. When the current value is valid,
  /// the output will depend on the DeltaControl provided.
  /// When delta_control is not provided or set to NULL_DOMINATES, a value following a
  /// null value returns null. When delta_control is set to VALUE_DOMINATES, a value following a
  /// null value returns the value. When delta_control is set to ZERO_DOMINATES, a value
  /// following a null value returns zero.
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.
  ///</param>
  /// <param name="deltaControl">defines how special cases should behave; defaults to VALUE_DOMINATES</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation Delta(IEnumerable<string> cols,
    DeltaControl deltaControl = DeltaControl.NullDominates) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Delta =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByDelta {
        Options = new UpdateByDeltaOptions {
          NullBehavior = ConvertDeltaControl(deltaControl)
        }
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates an EMA (exponential moving average) UpdateByOperation for the supplied column names,
  /// using ticks as the decay unit.
  /// The formula used is
  /// a = e^(-1 / decay_ticks)
  /// ema_next = a* ema_last + (1 - a) * value
  /// </summary>
  /// <param name="decayTicks">The decay rate in ticks</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="opControl">defines how special cases should behave; when unset, the default
  /// OperationControl() will be used.</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation EmaTick(double decayTicks, IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Ema = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEma {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(decayTicks)
    };
    return ubb.Build();
  }


  /// <summary>
  /// Creates an EMA (exponential moving average) UpdateByOperation for the supplied column names,
  /// using time as the decay unit.
  /// The formula used is
  /// a = e ^ (-dt / decay_time)
  /// ema_next = a * ema_last + (1 - a) * value
  /// </summary>
  /// <param name="timestampCol">the column in the source table to use for timestamps</param>
  /// <param name="decayTime">the decay rate, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="opControl">defines how special cases should behave; when unset, the default
  /// OperationControl() will be used.</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation EmaTime(string timestampCol, DurationSpecifier decayTime,
    IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Ema = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEma {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(timestampCol, decayTime)
    };
    return ubb.Build();
  }

  /// <summary>
  /// Creates an EMS (exponential moving sum) UpdateByOperation for the supplied column names, using ticks
  /// as the decay unit.
  /// The formula used is
  /// a = e^(-1 / decay_ticks)
  /// ems_next = a* ems_last + value
  /// </summary>
  /// <param name="decayTicks">the decay rate in ticks</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="opControl">defines how special cases should behave; when unset, the default
  /// OperationControl() will be used.</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation EmsTick(double decayTicks, IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Ems = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEms {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(decayTicks)
    };
    return ubb.Build();
  }

  /// <summary>
  /// Creates an EMS (exponential moving sum) UpdateByOperation for the supplied column names,
  /// using time as the decay unit.
  /// The formula used is
  /// a = e ^ (-dt / decay_time)
  /// ema_next = a * ema_last + (1 - a) * value
  /// </summary>
  /// <param name="timestampCol">the column in the source table to use for timestamps</param>
  /// <param name="decayTime">the decay rate, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="opControl">defines how special cases should behave; when unset, the default
  /// OperationControl() will be used.</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation EmsTime(string timestampCol, DurationSpecifier decayTime,
    IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Ems = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEms {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(timestampCol, decayTime)
    };
    return ubb.Build();
  }

  /// <summary>
  /// Creates an EM Min (exponential moving minimum) UpdateByOperation for the supplied column names, using ticks
  /// as the decay unit.
  /// The formula used is
  /// a = e^(-1 / decay_ticks)
  /// ems_next = a* ems_last + value
  /// </summary>
  /// <param name="decayTicks">the decay rate in ticks</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="opControl">defines how special cases should behave; when unset, the default
  /// OperationControl() will be used.</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation EmMinTick(double decayTicks, IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmMin = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmMin {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(decayTicks)
    };
    return ubb.Build();
  }

  /// <summary>
  /// Creates an EM Min (exponential moving minimum) UpdateByOperation for the supplied column names,
  /// using time as the decay unit.
  /// The formula used is
  /// a = e ^ (-dt / decay_time)
  /// ema_next = a * ema_last + (1 - a) * value
  /// </summary>
  /// <param name="timestampCol">the column in the source table to use for timestamps</param>
  /// <param name="decayTime">the decay rate, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="opControl">defines how special cases should behave; when unset, the default
  /// OperationControl() will be used.</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation EmMinTime(string timestampCol, DurationSpecifier decayTime,
    IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmMin = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmMin {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(timestampCol, decayTime)
    };
    return ubb.Build();
  }

  /// <summary>
  /// Creates an EM Max (exponential moving maximum) UpdateByOperation for the supplied column names, using ticks
  /// as the decay unit.
  /// The formula used is
  /// a = e^(-1 / decay_ticks)
  /// ems_next = a* ems_last + value
  /// </summary>
  /// <param name="decayTicks">the decay rate in ticks</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="opControl">defines how special cases should behave; when unset, the default
  /// OperationControl() will be used.</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation EmMaxTick(double decayTicks, IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmMax = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmMax {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(decayTicks)
    };
    return ubb.Build();
  }

  /// <summary>
  /// Creates an EM Max (exponential moving maximum) UpdateByOperation for the supplied column names,
  /// using time as the decay unit.
  /// The formula used is
  /// a = e ^ (-dt / decay_time)
  /// ema_next = a * ema_last + (1 - a) * value
  /// </summary>
  /// <param name="timestampCol">the column in the source table to use for timestamps</param>
  /// <param name="decayTime">the decay rate, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="opControl">defines how special cases should behave; when unset, the default
  /// OperationControl() will be used.</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation EmMaxTime(string timestampCol, DurationSpecifier decayTime,
    IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmMax = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmMax {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(timestampCol, decayTime)
    };
    return ubb.Build();
  }

  /// <summary>
  /// Creates an EM Std (exponential moving standard deviation) UpdateByOperation for the supplied column names, using ticks
  /// as the decay unit.
  /// The formula used is
  /// a = e^(-1 / decay_ticks)
  /// ems_next = a* ems_last + value
  /// </summary>
  /// <param name="decayTicks">the decay rate in ticks</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="opControl">defines how special cases should behave; when unset, the default
  /// OperationControl() will be used.</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation EmStdTick(double decayTicks, IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmStd = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmStd {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(decayTicks)
    };
    return ubb.Build();
  }

  /// <summary>
  /// Creates an EM Std (exponential moving standard deviation) UpdateByOperation for the supplied column names,
  /// using time as the decay unit.
  /// The formula used is
  /// a = e ^ (-dt / decay_time)
  /// ema_next = a * ema_last + (1 - a) * value
  /// </summary>
  /// <param name="timestampCol">the column in the source table to use for timestamps</param>
  /// <param name="decayTime">the decay rate, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="opControl">defines how special cases should behave; when unset, the default
  /// OperationControl() will be used.</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation EmStdTime(string timestampCol, DurationSpecifier decayTime,
    IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmStd = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmStd {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(timestampCol, decayTime)
    };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling sum UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
  /// are row counts, and you may specify the reverse and forward window in number of rows to include.The current row
  /// is considered to belong to the reverse window but not the forward window.Also, negative values are allowed and
  /// can be used to generate completely forward or completely reverse windows.
  /// Here are some examples of window values:
  /// <ul>
  /// <li>revTicks = 1, fwdTicks = 0 - contains only the current row</li>
  /// <li>revTicks = 10, fwdTicks = 0 - contains 9 previous rows and the current row</li>
  /// <li>>revTicks = 0, fwdTicks = 10 - contains the following 10 rows, excludes the current row</li>
  /// <li>>revTicks = 10, fwdTicks = 10 - contains the previous 9 rows, the current row and the 10 rows following</li>
  /// <li>>revTicks = 10, fwdTicks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
  /// current row (inclusive)</li>
  /// <li>>revTicks = 11, fwdTicks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
  /// current row (inclusive)</li>
  /// <li>revTicks = -5, fwdTicks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
  ///  current row (inclusive)</li>
  /// </ul>
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTicks">the look-behind window size (in rows/ticks)</param>
  /// <param name="fwdTicks">the look-forward window size (in rows/ticks)</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingSumTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingSum =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingSum {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling sum UpdateByOperation for the supplied column names, using time as the windowing unit. This
  /// function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
  /// allowed and can be used to generate completely forward or completely reverse windows.A row containing a null in
  /// the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
  /// be null.
  /// Here are some examples of window values:
  /// <li>revTime = 0, fwdTime = 0 - contains rows that exactly match the current row timestamp</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "0" - contains rows from 10m before through the current row timestamp
  /// (inclusive)</li>
  /// <li>revTime = 0, fwdTime = 600_000_000_000 - contains rows from the current row through 10m following the
  /// current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "PT00:10:00" - contains rows from 10m before through 10m following
  ///  the current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "-PT00:05:00" - contains rows from 10m before through 5m before the
  /// current row timestamp(inclusive), this is a purely backwards looking window</li>
  /// <li>revTime = "-PT00:05:00", fwdTime = "PT00:10:00"} - contains rows from 5m following through 10m
  /// following the current row timestamp(inclusive), this is a purely forwards looking window</li>
  /// </summary>
  /// <param name="timestampCol">the timestamp column for determining the window</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTime">the look-behind window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="fwdTime">the look-ahead window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M", default is 0</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingSumTime(string timestampCol, IEnumerable<string> cols,
  DurationSpecifier revTime, DurationSpecifier? fwdTime = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingSum =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingSum {
        ReverseWindowScale = MakeWindowScale(timestampCol, revTime),
        ForwardWindowScale = MakeWindowScale(timestampCol, fwdTime)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling group UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
  /// are row counts, and you may specify the reverse and forward window in number of rows to include.The current row
  /// is considered to belong to the reverse window but not the forward window.Also, negative values are allowed and
  /// can be used to generate completely forward or completely reverse windows.
  /// Here are some examples of window values:
  /// <ul>
  /// <li>revTicks = 1, fwdTicks = 0 - contains only the current row</li>
  /// <li>revTicks = 10, fwdTicks = 0 - contains 9 previous rows and the current row</li>
  /// <li>>revTicks = 0, fwdTicks = 10 - contains the following 10 rows, excludes the current row</li>
  /// <li>>revTicks = 10, fwdTicks = 10 - contains the previous 9 rows, the current row and the 10 rows following</li>
  /// <li>>revTicks = 10, fwdTicks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
  /// current row (inclusive)</li>
  /// <li>>revTicks = 11, fwdTicks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
  /// current row (inclusive)</li>
  /// <li>revTicks = -5, fwdTicks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
  ///  current row (inclusive)</li>
  /// </ul>
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTicks">the look-behind window size (in rows/ticks)</param>
  /// <param name="fwdTicks">the look-forward window size (in rows/ticks)</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingGroupTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingGroup =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingGroup {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling group UpdateByOperation for the supplied column names, using time as the windowing unit. This
  /// function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
  /// allowed and can be used to generate completely forward or completely reverse windows.A row containing a null in
  /// the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
  /// be null.
  /// Here are some examples of window values:
  /// <li>revTime = 0, fwdTime = 0 - contains rows that exactly match the current row timestamp</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "0" - contains rows from 10m before through the current row timestamp
  /// (inclusive)</li>
  /// <li>revTime = 0, fwdTime = 600_000_000_000 - contains rows from the current row through 10m following the
  /// current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "PT00:10:00" - contains rows from 10m before through 10m following
  ///  the current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "-PT00:05:00" - contains rows from 10m before through 5m before the
  /// current row timestamp(inclusive), this is a purely backwards looking window</li>
  /// <li>revTime = "-PT00:05:00", fwdTime = "PT00:10:00"} - contains rows from 5m following through 10m
  /// following the current row timestamp(inclusive), this is a purely forwards looking window</li>
  /// </summary>
  /// <param name="timestampCol">the timestamp column for determining the window</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTime">the look-behind window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="fwdTime">the look-ahead window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M", default is 0</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingGroupTime(string timestampCol, IEnumerable<string> cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingGroup =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingGroup {
        ReverseWindowScale = MakeWindowScale(timestampCol, revTime),
        ForwardWindowScale = MakeWindowScale(timestampCol, fwdTime)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling average UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
  /// are row counts, and you may specify the reverse and forward window in number of rows to include.The current row
  /// is considered to belong to the reverse window but not the forward window.Also, negative values are allowed and
  /// can be used to generate completely forward or completely reverse windows.
  /// Here are some examples of window values:
  /// <ul>
  /// <li>revTicks = 1, fwdTicks = 0 - contains only the current row</li>
  /// <li>revTicks = 10, fwdTicks = 0 - contains 9 previous rows and the current row</li>
  /// <li>>revTicks = 0, fwdTicks = 10 - contains the following 10 rows, excludes the current row</li>
  /// <li>>revTicks = 10, fwdTicks = 10 - contains the previous 9 rows, the current row and the 10 rows following</li>
  /// <li>>revTicks = 10, fwdTicks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
  /// current row (inclusive)</li>
  /// <li>>revTicks = 11, fwdTicks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
  /// current row (inclusive)</li>
  /// <li>revTicks = -5, fwdTicks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
  ///  current row (inclusive)</li>
  /// </ul>
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTicks">the look-behind window size (in rows/ticks)</param>
  /// <param name="fwdTicks">the look-forward window size (in rows/ticks)</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingAvgTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingAvg =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingAvg {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling average UpdateByOperation for the supplied column names, using time as the windowing unit. This
  /// function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
  /// allowed and can be used to generate completely forward or completely reverse windows.A row containing a null in
  /// the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
  /// be null.
  /// Here are some examples of window values:
  /// <li>revTime = 0, fwdTime = 0 - contains rows that exactly match the current row timestamp</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "0" - contains rows from 10m before through the current row timestamp
  /// (inclusive)</li>
  /// <li>revTime = 0, fwdTime = 600_000_000_000 - contains rows from the current row through 10m following the
  /// current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "PT00:10:00" - contains rows from 10m before through 10m following
  ///  the current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "-PT00:05:00" - contains rows from 10m before through 5m before the
  /// current row timestamp(inclusive), this is a purely backwards looking window</li>
  /// <li>revTime = "-PT00:05:00", fwdTime = "PT00:10:00"} - contains rows from 5m following through 10m
  /// following the current row timestamp(inclusive), this is a purely forwards looking window</li>
  /// </summary>
  /// <param name="timestampCol">the timestamp column for determining the window</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTime">the look-behind window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="fwdTime">the look-ahead window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M", default is 0</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingAvgTime(string timestampCol, IEnumerable<string> cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingAvg =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingAvg {
        ReverseWindowScale = MakeWindowScale(timestampCol, revTime),
        ForwardWindowScale = MakeWindowScale(timestampCol, fwdTime)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling minimum UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
  /// are row counts, and you may specify the reverse and forward window in number of rows to include.The current row
  /// is considered to belong to the reverse window but not the forward window.Also, negative values are allowed and
  /// can be used to generate completely forward or completely reverse windows.
  /// Here are some examples of window values:
  /// <ul>
  /// <li>revTicks = 1, fwdTicks = 0 - contains only the current row</li>
  /// <li>revTicks = 10, fwdTicks = 0 - contains 9 previous rows and the current row</li>
  /// <li>>revTicks = 0, fwdTicks = 10 - contains the following 10 rows, excludes the current row</li>
  /// <li>>revTicks = 10, fwdTicks = 10 - contains the previous 9 rows, the current row and the 10 rows following</li>
  /// <li>>revTicks = 10, fwdTicks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
  /// current row (inclusive)</li>
  /// <li>>revTicks = 11, fwdTicks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
  /// current row (inclusive)</li>
  /// <li>revTicks = -5, fwdTicks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
  ///  current row (inclusive)</li>
  /// </ul>
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTicks">the look-behind window size (in rows/ticks)</param>
  /// <param name="fwdTicks">the look-forward window size (in rows/ticks)</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingMinTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingMin =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingMin {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling minimum UpdateByOperation for the supplied column names, using time as the windowing unit. This
  /// function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
  /// allowed and can be used to generate completely forward or completely reverse windows.A row containing a null in
  /// the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
  /// be null.
  /// Here are some examples of window values:
  /// <li>revTime = 0, fwdTime = 0 - contains rows that exactly match the current row timestamp</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "0" - contains rows from 10m before through the current row timestamp
  /// (inclusive)</li>
  /// <li>revTime = 0, fwdTime = 600_000_000_000 - contains rows from the current row through 10m following the
  /// current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "PT00:10:00" - contains rows from 10m before through 10m following
  ///  the current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "-PT00:05:00" - contains rows from 10m before through 5m before the
  /// current row timestamp(inclusive), this is a purely backwards looking window</li>
  /// <li>revTime = "-PT00:05:00", fwdTime = "PT00:10:00"} - contains rows from 5m following through 10m
  /// following the current row timestamp(inclusive), this is a purely forwards looking window</li>
  /// </summary>
  /// <param name="timestampCol">the timestamp column for determining the window</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTime">the look-behind window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="fwdTime">the look-ahead window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M", default is 0</param>
  public static UpdateByOperation RollingMinTime(string timestampCol, IEnumerable<string> cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingMin =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingMin {
        ReverseWindowScale = MakeWindowScale(timestampCol, revTime),
        ForwardWindowScale = MakeWindowScale(timestampCol, fwdTime)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling maximum UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
  /// are row counts, and you may specify the reverse and forward window in number of rows to include.The current row
  /// is considered to belong to the reverse window but not the forward window.Also, negative values are allowed and
  /// can be used to generate completely forward or completely reverse windows.
  /// Here are some examples of window values:
  /// <ul>
  /// <li>revTicks = 1, fwdTicks = 0 - contains only the current row</li>
  /// <li>revTicks = 10, fwdTicks = 0 - contains 9 previous rows and the current row</li>
  /// <li>>revTicks = 0, fwdTicks = 10 - contains the following 10 rows, excludes the current row</li>
  /// <li>>revTicks = 10, fwdTicks = 10 - contains the previous 9 rows, the current row and the 10 rows following</li>
  /// <li>>revTicks = 10, fwdTicks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
  /// current row (inclusive)</li>
  /// <li>>revTicks = 11, fwdTicks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
  /// current row (inclusive)</li>
  /// <li>revTicks = -5, fwdTicks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
  ///  current row (inclusive)</li>
  /// </ul>
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTicks">the look-behind window size (in rows/ticks)</param>
  /// <param name="fwdTicks">the look-forward window size (in rows/ticks)</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingMaxTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingMax =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingMax {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling maximum UpdateByOperation for the supplied column names, using time as the windowing unit. This
  /// function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
  /// allowed and can be used to generate completely forward or completely reverse windows.A row containing a null in
  /// the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
  /// be null.
  /// Here are some examples of window values:
  /// <li>revTime = 0, fwdTime = 0 - contains rows that exactly match the current row timestamp</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "0" - contains rows from 10m before through the current row timestamp
  /// (inclusive)</li>
  /// <li>revTime = 0, fwdTime = 600_000_000_000 - contains rows from the current row through 10m following the
  /// current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "PT00:10:00" - contains rows from 10m before through 10m following
  ///  the current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "-PT00:05:00" - contains rows from 10m before through 5m before the
  /// current row timestamp(inclusive), this is a purely backwards looking window</li>
  /// <li>revTime = "-PT00:05:00", fwdTime = "PT00:10:00"} - contains rows from 5m following through 10m
  /// following the current row timestamp(inclusive), this is a purely forwards looking window</li>
  /// </summary>
  /// <param name="timestampCol">the timestamp column for determining the window</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTime">the look-behind window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="fwdTime">the look-ahead window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M", default is 0</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingMaxTime(string timestampCol, IEnumerable<string> cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingMax =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingMax {
        ReverseWindowScale = MakeWindowScale(timestampCol, revTime),
        ForwardWindowScale = MakeWindowScale(timestampCol, fwdTime)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling product UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
  /// are row counts, and you may specify the reverse and forward window in number of rows to include.The current row
  /// is considered to belong to the reverse window but not the forward window.Also, negative values are allowed and
  /// can be used to generate completely forward or completely reverse windows.
  /// Here are some examples of window values:
  /// <ul>
  /// <li>revTicks = 1, fwdTicks = 0 - contains only the current row</li>
  /// <li>revTicks = 10, fwdTicks = 0 - contains 9 previous rows and the current row</li>
  /// <li>>revTicks = 0, fwdTicks = 10 - contains the following 10 rows, excludes the current row</li>
  /// <li>>revTicks = 10, fwdTicks = 10 - contains the previous 9 rows, the current row and the 10 rows following</li>
  /// <li>>revTicks = 10, fwdTicks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
  /// current row (inclusive)</li>
  /// <li>>revTicks = 11, fwdTicks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
  /// current row (inclusive)</li>
  /// <li>revTicks = -5, fwdTicks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
  ///  current row (inclusive)</li>
  /// </ul>
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTicks">the look-behind window size (in rows/ticks)</param>
  /// <param name="fwdTicks">the look-forward window size (in rows/ticks)</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingProdTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingProduct =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingProduct {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling product UpdateByOperation for the supplied column names, using time as the windowing unit. This
  /// function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
  /// allowed and can be used to generate completely forward or completely reverse windows.A row containing a null in
  /// the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
  /// be null.
  /// Here are some examples of window values:
  /// <li>revTime = 0, fwdTime = 0 - contains rows that exactly match the current row timestamp</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "0" - contains rows from 10m before through the current row timestamp
  /// (inclusive)</li>
  /// <li>revTime = 0, fwdTime = 600_000_000_000 - contains rows from the current row through 10m following the
  /// current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "PT00:10:00" - contains rows from 10m before through 10m following
  ///  the current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "-PT00:05:00" - contains rows from 10m before through 5m before the
  /// current row timestamp(inclusive), this is a purely backwards looking window</li>
  /// <li>revTime = "-PT00:05:00", fwdTime = "PT00:10:00"} - contains rows from 5m following through 10m
  /// following the current row timestamp(inclusive), this is a purely forwards looking window</li>
  /// </summary>
  /// <param name="timestampCol">the timestamp column for determining the window</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTime">the look-behind window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="fwdTime">the look-ahead window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M", default is 0</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingProdTime(string timestampCol, IEnumerable<string> cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingProduct =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingProduct {
        ReverseWindowScale = MakeWindowScale(timestampCol, revTime),
        ForwardWindowScale = MakeWindowScale(timestampCol, fwdTime)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling count UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
  /// are row counts, and you may specify the reverse and forward window in number of rows to include.The current row
  /// is considered to belong to the reverse window but not the forward window.Also, negative values are allowed and
  /// can be used to generate completely forward or completely reverse windows.
  /// Here are some examples of window values:
  /// <ul>
  /// <li>revTicks = 1, fwdTicks = 0 - contains only the current row</li>
  /// <li>revTicks = 10, fwdTicks = 0 - contains 9 previous rows and the current row</li>
  /// <li>>revTicks = 0, fwdTicks = 10 - contains the following 10 rows, excludes the current row</li>
  /// <li>>revTicks = 10, fwdTicks = 10 - contains the previous 9 rows, the current row and the 10 rows following</li>
  /// <li>>revTicks = 10, fwdTicks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
  /// current row (inclusive)</li>
  /// <li>>revTicks = 11, fwdTicks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
  /// current row (inclusive)</li>
  /// <li>revTicks = -5, fwdTicks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
  ///  current row (inclusive)</li>
  /// </ul>
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTicks">the look-behind window size (in rows/ticks)</param>
  /// <param name="fwdTicks">the look-forward window size (in rows/ticks)</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingCountTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingCount =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingCount {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling count UpdateByOperation for the supplied column names, using time as the windowing unit. This
  /// function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
  /// allowed and can be used to generate completely forward or completely reverse windows.A row containing a null in
  /// the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
  /// be null.
  /// Here are some examples of window values:
  /// <li>revTime = 0, fwdTime = 0 - contains rows that exactly match the current row timestamp</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "0" - contains rows from 10m before through the current row timestamp
  /// (inclusive)</li>
  /// <li>revTime = 0, fwdTime = 600_000_000_000 - contains rows from the current row through 10m following the
  /// current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "PT00:10:00" - contains rows from 10m before through 10m following
  ///  the current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "-PT00:05:00" - contains rows from 10m before through 5m before the
  /// current row timestamp(inclusive), this is a purely backwards looking window</li>
  /// <li>revTime = "-PT00:05:00", fwdTime = "PT00:10:00"} - contains rows from 5m following through 10m
  /// following the current row timestamp(inclusive), this is a purely forwards looking window</li>
  /// </summary>
  /// <param name="timestampCol">the timestamp column for determining the window</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTime">the look-behind window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="fwdTime">the look-ahead window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M", default is 0</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingCountTime(string timestampCol, IEnumerable<string> cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingCount =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingCount {
        ReverseWindowScale = MakeWindowScale(timestampCol, revTime),
        ForwardWindowScale = MakeWindowScale(timestampCol, fwdTime)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling standard deviation UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
  /// are row counts, and you may specify the reverse and forward window in number of rows to include.The current row
  /// is considered to belong to the reverse window but not the forward window.Also, negative values are allowed and
  /// can be used to generate completely forward or completely reverse windows.
  /// Here are some examples of window values:
  /// <ul>
  /// <li>revTicks = 1, fwdTicks = 0 - contains only the current row</li>
  /// <li>revTicks = 10, fwdTicks = 0 - contains 9 previous rows and the current row</li>
  /// <li>>revTicks = 0, fwdTicks = 10 - contains the following 10 rows, excludes the current row</li>
  /// <li>>revTicks = 10, fwdTicks = 10 - contains the previous 9 rows, the current row and the 10 rows following</li>
  /// <li>>revTicks = 10, fwdTicks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
  /// current row (inclusive)</li>
  /// <li>>revTicks = 11, fwdTicks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
  /// current row (inclusive)</li>
  /// <li>revTicks = -5, fwdTicks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
  ///  current row (inclusive)</li>
  /// </ul>
  /// </summary>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTicks">the look-behind window size (in rows/ticks)</param>
  /// <param name="fwdTicks">the look-forward window size (in rows/ticks)</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingStdTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingStd =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingStd {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling standard deviation UpdateByOperation for the supplied column names, using time as the windowing unit. This
  /// function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
  /// allowed and can be used to generate completely forward or completely reverse windows.A row containing a null in
  /// the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
  /// be null.
  /// Here are some examples of window values:
  /// <li>revTime = 0, fwdTime = 0 - contains rows that exactly match the current row timestamp</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "0" - contains rows from 10m before through the current row timestamp
  /// (inclusive)</li>
  /// <li>revTime = 0, fwdTime = 600_000_000_000 - contains rows from the current row through 10m following the
  /// current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "PT00:10:00" - contains rows from 10m before through 10m following
  ///  the current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "-PT00:05:00" - contains rows from 10m before through 5m before the
  /// current row timestamp(inclusive), this is a purely backwards looking window</li>
  /// <li>revTime = "-PT00:05:00", fwdTime = "PT00:10:00"} - contains rows from 5m following through 10m
  /// following the current row timestamp(inclusive), this is a purely forwards looking window</li>
  /// </summary>
  /// <param name="timestampCol">the timestamp column for determining the window</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTime">the look-behind window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="fwdTime">the look-ahead window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M", default is 0</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingStdTime(string timestampCol, IEnumerable<string> cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingStd =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingStd {
        ReverseWindowScale = MakeWindowScale(timestampCol, revTime),
        ForwardWindowScale = MakeWindowScale(timestampCol, fwdTime)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling weighted average UpdateByOperation for the supplied column names, using ticks as the windowing unit. Ticks
  /// are row counts, and you may specify the reverse and forward window in number of rows to include.The current row
  /// is considered to belong to the reverse window but not the forward window.Also, negative values are allowed and
  /// can be used to generate completely forward or completely reverse windows.
  /// Here are some examples of window values:
  /// <ul>
  /// <li>revTicks = 1, fwdTicks = 0 - contains only the current row</li>
  /// <li>revTicks = 10, fwdTicks = 0 - contains 9 previous rows and the current row</li>
  /// <li>>revTicks = 0, fwdTicks = 10 - contains the following 10 rows, excludes the current row</li>
  /// <li>>revTicks = 10, fwdTicks = 10 - contains the previous 9 rows, the current row and the 10 rows following</li>
  /// <li>>revTicks = 10, fwdTicks = -5 - contains 5 rows, beginning at 9 rows before, ending at 5 rows before the
  /// current row (inclusive)</li>
  /// <li>>revTicks = 11, fwdTicks = -1 - contains 10 rows, beginning at 10 rows before, ending at 1 row before the
  /// current row (inclusive)</li>
  /// <li>revTicks = -5, fwdTicks = 10 - contains 5 rows, beginning 5 rows following, ending at 10 rows following the
  ///  current row (inclusive)</li>
  /// </ul>
  /// </summary>
  /// <param name="weightCol">The column containing the weight valutes</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTicks">the look-behind window size (in rows/ticks)</param>
  /// <param name="fwdTicks">the look-forward window size (in rows/ticks)</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingWavgTick(string weightCol, IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingWavg =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingWAvg {
        WeightColumn = weightCol,
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

  /// <summary>
  /// Creates a rolling group UpdateByOperation for the supplied column names, using time as the windowing unit. This
  /// function accepts nanoseconds or time strings as the reverse and forward window parameters. Negative values are
  /// allowed and can be used to generate completely forward or completely reverse windows.A row containing a null in
  /// the timestamp column belongs to no window and will not be considered in the windows of other rows; its output will
  /// be null.
  /// Here are some examples of window values:
  /// <li>revTime = 0, fwdTime = 0 - contains rows that exactly match the current row timestamp</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "0" - contains rows from 10m before through the current row timestamp
  /// (inclusive)</li>
  /// <li>revTime = 0, fwdTime = 600_000_000_000 - contains rows from the current row through 10m following the
  /// current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "PT00:10:00" - contains rows from 10m before through 10m following
  ///  the current row timestamp(inclusive)</li>
  /// <li>revTime = "PT00:10:00", fwdTime = "-PT00:05:00" - contains rows from 10m before through 5m before the
  /// current row timestamp(inclusive), this is a purely backwards looking window</li>
  /// <li>revTime = "-PT00:05:00", fwdTime = "PT00:10:00"} - contains rows from 5m following through 10m
  /// following the current row timestamp(inclusive), this is a purely forwards looking window</li>
  /// </summary>
  /// <param name="timestampCol">the timestamp column for determining the window</param>
  /// <param name="weightCol">The column containing the weight valutes</param>
  /// <param name="cols">the column(s) to be operated on, can include expressions to rename the output,
  /// i.e. "new_col = col"; when empty, update_by performs the operation on all applicable columns.</param>
  /// <param name="revTime">the look-behind window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M"</param>
  /// <param name="fwdTime">the look-ahead window size, can be expressed as an integer in nanoseconds or a time
  /// interval string, e.g. "PT00:00:.001" or "PT5M", default is 0</param>
  /// <returns>The UpdateByOperation</returns>
  public static UpdateByOperation RollingWavgTime(string timestampCol, string weightCol, IEnumerable<string> cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingWavg =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingWAvg {
        WeightColumn = weightCol,
        ReverseWindowScale = MakeWindowScale(timestampCol, revTime),
        ForwardWindowScale = MakeWindowScale(timestampCol, fwdTime)
      };
    return ubb.Build();
  }

  private static UpdateByNullBehavior ConvertDeltaControl(DeltaControl dc) {
    return dc switch {
      DeltaControl.NullDominates => UpdateByNullBehavior.NullDominates,
      DeltaControl.ValueDominates => UpdateByNullBehavior.ValueDominates,
      DeltaControl.ZeroDominates => UpdateByNullBehavior.ZeroDominates,
      _ => throw new Exception($"Unexpected DeltaControl {dc}")
    };
  }

  private static UpdateByEmOptions ConvertOperationControl(OperationControl? oc) {
    oc ??= new OperationControl();
    var result = new UpdateByEmOptions {
      OnNullValue = ConvertBadDataBehavior(oc.OnNull),
      OnNanValue = ConvertBadDataBehavior(oc.OnNan),
      BigValueContext = ConvertMathContext(oc.BigValueContext)
    };
    return result;
  }

  private static BadDataBehaviorProtoEnum ConvertBadDataBehavior(BadDataBehavior bdb) {
    return bdb switch {
      BadDataBehavior.Reset => BadDataBehaviorProtoEnum.Reset,
      BadDataBehavior.Skip => BadDataBehaviorProtoEnum.Skip,
      BadDataBehavior.Throw => BadDataBehaviorProtoEnum.Throw,
      BadDataBehavior.Poison => BadDataBehaviorProtoEnum.Poison,
      _ => throw new Exception($"Unexpected BadDataBehavior {bdb}")
    };
  }

  private static MathContextProto ConvertMathContext(MathContext mctx) {
    var (precision, roundingMode) = mctx switch {
      // For the values used here, please see the documentation for java.math.MathContext:
      // https://docs.oracle.com/javase/8/docs/api/java/math/MathContext.html

      // "A MathContext object whose settings have the values required for unlimited precision arithmetic."
      MathContext.Unlimited => (0, RoundingModeProtoEnum.HalfUp),

      // "A MathContext object with a precision setting matching the IEEE 754R Decimal32 format, 7 digits, and a rounding mode of HALF_EVEN, the IEEE 754R default."
      MathContext.Decimal32 => (7, RoundingModeProtoEnum.HalfEven),

      // "A MathContext object with a precision setting matching the IEEE 754R Decimal64 format, 16 digits, and a rounding mode of HALF_EVEN, the IEEE 754R default."
      MathContext.Decimal64 => (16, RoundingModeProtoEnum.HalfEven),

      // "A MathContext object with a precision setting matching the IEEE 754R Decimal128 format, 34 digits, and a rounding mode of HALF_EVEN, the IEEE 754R default."
      MathContext.Decimal128 => (34, RoundingModeProtoEnum.HalfEven),

      _ => throw new Exception($"Unexpected MathContext {mctx}")
    };
    var result = new MathContextProto {
      Precision = precision,
      RoundingMode = roundingMode
    };
    return result;
  }


  private static UpdateByWindowScale MakeWindowScale(double ticks) {
    return new UpdateByWindowScale {
      Ticks = new UpdateByWindowScale.Types.UpdateByWindowTicks {
        Ticks = ticks
      }
    };
  }

  private static UpdateByWindowScale MakeWindowScale(string timestampCol, DurationSpecifier? decayTime) {
    var result = new UpdateByWindowScale {
      Time = new UpdateByWindowScale.Types.UpdateByWindowTime {
        Column = timestampCol
      }
    };

    var decayTimeToUse = decayTime ?? new DurationSpecifier();

    decayTimeToUse.Visit(
      nanos => result.Time.Nanos = nanos,
      duration => result.Time.DurationString = duration
      );

    return result;
  }
}

internal class UpdateByBuilder {
  private readonly UpdateByOperationProto _gup = new();

  public UpdateByBuilder(IEnumerable<string> cols) {
    _gup.Column ??= new UpdateByOperationProto.Types.UpdateByColumn();
    _gup.Column.MatchPairs.AddRange(cols);
  }

  public UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec MutableColumnSpec() {
    _gup.Column ??= new UpdateByOperationProto.Types.UpdateByColumn();
    _gup.Column.Spec ??= new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec();
    return _gup.Column.Spec;
  }

  public UpdateByOperation Build() {
    return new UpdateByOperation(_gup);
  }
}

  