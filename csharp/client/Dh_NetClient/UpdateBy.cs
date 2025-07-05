//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
using Io.Deephaven.Proto.Backplane.Grpc;
using System;
using UpdateByOperationProto = Io.Deephaven.Proto.Backplane.Grpc.UpdateByRequest.Types.UpdateByOperation;
using BadDataBehaviorProtoEnum = Io.Deephaven.Proto.Backplane.Grpc.BadDataBehavior;
using MathContextProto = Io.Deephaven.Proto.Backplane.Grpc.MathContext;
using RoundingModeProtoEnum = Io.Deephaven.Proto.Backplane.Grpc.MathContext.Types.RoundingMode;

namespace Deephaven.Dh_NetClient;

public enum MathContext : Int32 {
  Unlimited, Decimal32, Decimal64, Decimal128
}

public enum BadDataBehavior : Int32 {
  Reset, Skip, Throw, Poison
}

public enum DeltaControl : Int32 {
  NullDominates, ValueDominates, ZeroDominates
}

public record OperationControl(
  BadDataBehavior OnNull = BadDataBehavior.Skip,
  BadDataBehavior OnNan = BadDataBehavior.Skip,
  MathContext BigValueContext = MathContext.Decimal128);

public class UpdateByOperation {
  public readonly UpdateByOperationProto UpdateByProto;

  public UpdateByOperation(UpdateByRequest.Types.UpdateByOperation updateByProto) {
    UpdateByProto = updateByProto;
  }

  public static UpdateByOperation CumSum(params string[] cols) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Sum =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByCumulativeSum();
    return ubb.Build();
  }

  public static UpdateByOperation CumProd(params string[] cols) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Product = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByCumulativeProduct();
    return ubb.Build();
  }

  public static UpdateByOperation CumMin(params string[] cols) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Min = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByCumulativeMin();
    return ubb.Build();
  }

  public static UpdateByOperation CumMax(params string[] cols) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Max = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByCumulativeMax();
    return ubb.Build();
  }

  public static UpdateByOperation ForwardFill(params string[] cols) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Fill = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByFill();
    return ubb.Build();
  }

  public static UpdateByOperation Delta(IEnumerable<string> cols, DeltaControl deltaControl = DeltaControl.NullDominates) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Delta =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByDelta {
        Options = new UpdateByDeltaOptions {
          NullBehavior = ConvertDeltaControl(deltaControl)
        }
      };
    return ubb.Build();
  }

  public static UpdateByOperation EmaTick(double decayTicks, IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Ema = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEma {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(decayTicks)
    };
    return ubb.Build();
  }

  public static UpdateByOperation EmaTime(string timestampCol, DurationSpecifier decayTime,
    IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Ema = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEma {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(timestampCol, decayTime)
    };
    return ubb.Build();
  }

  public static UpdateByOperation EmsTick(double decayTicks, IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Ems = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEms {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(decayTicks)
    };
    return ubb.Build();
  }

  public static UpdateByOperation EmsTime(string timestampCol, DurationSpecifier decayTime,
    IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().Ems = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEms {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(timestampCol, decayTime)
    };
    return ubb.Build();
  }

  public static UpdateByOperation EmMinTick(double decayTicks, IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmMin = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmMin {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(decayTicks)
    };
    return ubb.Build();
  }

  public static UpdateByOperation EmMinTime(string timestampCol, DurationSpecifier decayTime,
    IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmMin = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmMin {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(timestampCol, decayTime)
    };
    return ubb.Build();
  }

  public static UpdateByOperation EmMaxTick(double decayTicks, IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmMax = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmMax {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(decayTicks)
    };
    return ubb.Build();
  }

  public static UpdateByOperation EmMaxTime(string timestampCol, DurationSpecifier decayTime,
    IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmMax = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmMax {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(timestampCol, decayTime)
    };
    return ubb.Build();
  }

  public static UpdateByOperation EmStdTick(double decayTicks, IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmStd = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmStd {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(decayTicks)
    };
    return ubb.Build();
  }

  public static UpdateByOperation EmStdTime(string timestampCol, DurationSpecifier decayTime,
    IEnumerable<string> cols, OperationControl? opControl = null) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().EmStd = new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByEmStd {
      Options = ConvertOperationControl(opControl),
      WindowScale = MakeWindowScale(timestampCol, decayTime)
    };
    return ubb.Build();
  }

  public static UpdateByOperation RollingSumTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingSum =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingSum {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

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

  public static UpdateByOperation RollingGroupTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingGroup =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingGroup {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

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

  public static UpdateByOperation RollingAvgTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingAvg =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingAvg {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

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

  public static UpdateByOperation RollingMinTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingMin =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingMin {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

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

  public static UpdateByOperation RollingMaxTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingMax =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingMax {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

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

  public static UpdateByOperation RollingProdTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingProduct =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingProduct {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

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

  public static UpdateByOperation RollingCountTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingCount =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingCount {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

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

  public static UpdateByOperation RollingStdTick(IEnumerable<string> cols, int revTicks, int fwdTicks = 0) {
    var ubb = new UpdateByBuilder(cols);
    ubb.MutableColumnSpec().RollingStd =
      new UpdateByOperationProto.Types.UpdateByColumn.Types.UpdateBySpec.Types.UpdateByRollingStd {
        ReverseWindowScale = MakeWindowScale(revTicks),
        ForwardWindowScale = MakeWindowScale(fwdTicks)
      };
    return ubb.Build();
  }

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
