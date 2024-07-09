using System;
using System.Runtime.InteropServices;
using Deephaven.DeephavenClient.Interop;

namespace Deephaven.DeephavenClient.UpdateBy;

public enum MathContext : Int32 {
  Unlimited, Decimal32, Decimal64, Decimal128
};

public enum BadDataBehavior : Int32 {
  Reset, Skip, Throw, Poison
};

public enum DeltaControl : Int32 {
  NullDominates, ValueDominates, ZeroDominates
};

public readonly struct OperationControl {
  public readonly BadDataBehavior OnNull;
  public readonly BadDataBehavior OnNaN;
  public readonly MathContext BigValueContext;

  public OperationControl(BadDataBehavior onNull = BadDataBehavior.Skip,
    BadDataBehavior onNaN = BadDataBehavior.Skip,
    MathContext bigValueContext = MathContext.Decimal128) {
    OnNull = onNull;
    OnNaN = onNaN;
    BigValueContext = bigValueContext;
  }
}

public abstract class UpdateByOperation {
  internal abstract InternalUpdateByOperation MakeInternal();

  private sealed class WithCols : UpdateByOperation {
    public delegate void NativeInvoker(string[] cols, Int32 numCols,
      out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);

    private readonly string[] _cols;
    private readonly NativeInvoker _invoker;

    public WithCols(string[] cols, NativeInvoker invoker) {
      _cols = cols;
      _invoker = invoker;
    }

    internal override InternalUpdateByOperation MakeInternal() {
      _invoker(_cols, _cols.Length, out var result, out var status);
      status.OkOrThrow();
      return new InternalUpdateByOperation(result);
    }
  }

  private sealed class WithDelta : UpdateByOperation {
    public delegate void NativeInvoker(string[] cols, Int32 numCols, DeltaControl deltaControl,
      out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);

    private readonly string[] _cols;
    private readonly DeltaControl _deltaControl;
    private readonly NativeInvoker _invoker;

    public WithDelta(string[] cols, DeltaControl deltaControl, NativeInvoker invoker) {
      _cols = cols;
      _deltaControl = deltaControl;
      _invoker = invoker;
    }

    internal override InternalUpdateByOperation MakeInternal() {
      _invoker(_cols, _cols.Length, _deltaControl, out var result, out var status);
      status.OkOrThrow();
      return new InternalUpdateByOperation(result);
    }
  }

  private sealed class WithTicks : UpdateByOperation {
    public delegate void NativeInvoker(double decayTicks, string[] cols, Int32 numCols,
      ref OperationControl operationControl,
      out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);

    private readonly double _decayTicks;
    private readonly string[] _cols;
    private OperationControl _operationControl;
    private readonly NativeInvoker _invoker;

    public WithTicks(double decayTicks, string[] cols, OperationControl? operationControl,
      NativeInvoker invoker) {
      _decayTicks = decayTicks;
      _cols = cols;
      _operationControl = operationControl ?? new OperationControl();
      _invoker = invoker;
    }

    internal override InternalUpdateByOperation MakeInternal() {
      _invoker(_decayTicks, _cols, _cols.Length, ref _operationControl, out var result, out var status);
      status.OkOrThrow();
      return new InternalUpdateByOperation(result);
    }
  }

  private sealed class WithTime : UpdateByOperation {
    public delegate void NativeInvoker(string timestampCol, 
      NativePtr<NativeDurationSpecifier> decayTime, string[] cols, Int32 numCols,
      ref OperationControl operationControl,
      out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);

    private readonly string _timestampCol;
    private readonly DurationSpecifier _decayTime;
    private readonly string[] _cols;
    private OperationControl _operationControl;
    private readonly NativeInvoker _invoker;

    public WithTime(string timestampCol, DurationSpecifier decayTime, string[] cols, OperationControl? operationControl,
      NativeInvoker invoker) {
      _timestampCol = timestampCol;
      _decayTime = decayTime;
      _cols = cols;
      _operationControl = operationControl ?? new OperationControl();
      _invoker = invoker;
    }

    internal override InternalUpdateByOperation MakeInternal() {
      using var dc = _decayTime.Materialize();
      _invoker(_timestampCol, dc.Self, _cols, _cols.Length, ref _operationControl, out var result, out var status);
      status.OkOrThrow();
      return new InternalUpdateByOperation(result);
    }
  }

  private sealed class WithRollingTicks : UpdateByOperation {
    public delegate void NativeInvoker(string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
      out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);

    private readonly string[] _cols;
    private readonly Int32 _revTicks;
    private readonly Int32 _fwdTicks;
    private readonly NativeInvoker _invoker;

    public WithRollingTicks(string[] cols, Int32 revTicks, Int32 fwdTicks, NativeInvoker invoker) {
      _cols = cols;
      _revTicks = revTicks;
      _fwdTicks = fwdTicks;
      _invoker = invoker;
    }

    internal override InternalUpdateByOperation MakeInternal() {
      _invoker(_cols, _cols.Length, _revTicks, _fwdTicks, out var result, out var status);
      status.OkOrThrow();
      return new InternalUpdateByOperation(result);
    }
  }

  private sealed class WithRollingTime : UpdateByOperation {
    public delegate void NativeInvoker(string timestampCol, string[] cols, Int32 numCols,
      NativePtr<NativeDurationSpecifier> revTime,
      NativePtr<NativeDurationSpecifier> fwdTime,
      out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);

    private readonly string _timestampCol;
    private readonly string[] _cols;
    private readonly DurationSpecifier _revTime;
    private readonly DurationSpecifier _fwdTime;
    private readonly NativeInvoker _invoker;

    public WithRollingTime(string timestampCol, string[] cols,
      DurationSpecifier revTime, DurationSpecifier? fwdTime, NativeInvoker invoker) {
      _timestampCol = timestampCol;
      _cols = cols;
      _revTime = revTime;
      _fwdTime = fwdTime ?? new DurationSpecifier(0);
      _invoker = invoker;
    }

    internal override InternalUpdateByOperation MakeInternal() {
      using var revNative = _revTime.Materialize();
      using var fwdNative = _fwdTime.Materialize();
      _invoker(_timestampCol, _cols, _cols.Length, revNative.Self, fwdNative.Self,
        out var result, out var status);
      status.OkOrThrow();
      return new InternalUpdateByOperation(result);
    }
  }

  private sealed class WithRollingWavgTicks : UpdateByOperation {
    public delegate void NativeInvoker(string weightCol, string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
      out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);

    private readonly string _weightCol;
    private readonly string[] _cols;
    private readonly Int32 _revTicks;
    private readonly Int32 _fwdTicks;
    private readonly NativeInvoker _invoker;

    public WithRollingWavgTicks(string weightCol, string[] cols, Int32 revTicks, Int32 fwdTicks, NativeInvoker invoker) {
      _weightCol = weightCol;
      _cols = cols;
      _revTicks = revTicks;
      _fwdTicks = fwdTicks;
      _invoker = invoker;
    }

    internal override InternalUpdateByOperation MakeInternal() {
      _invoker(_weightCol, _cols, _cols.Length, _revTicks, _fwdTicks, out var result, out var status);
      status.OkOrThrow();
      return new InternalUpdateByOperation(result);
    }
  }

  private sealed class WithRollingWavgTime : UpdateByOperation {
    public delegate void NativeInvoker(string timestampCol, string weightCol, string[] cols, Int32 numCols,
      NativePtr<NativeDurationSpecifier> revTime,
      NativePtr<NativeDurationSpecifier> fwdTime,
      out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);

    private readonly string _timestampCol;
    private readonly string _weightCol;
    private readonly string[] _cols;
    private readonly DurationSpecifier _revTime;
    private readonly DurationSpecifier _fwdTime;
    private readonly NativeInvoker _invoker;

    public WithRollingWavgTime(string timestampCol, string weightCol, string[] cols,
      DurationSpecifier revTime, DurationSpecifier? fwdTime, NativeInvoker invoker) {
      _timestampCol = timestampCol;
      _weightCol = weightCol;
      _cols = cols;
      _revTime = revTime;
      _fwdTime = fwdTime ?? new DurationSpecifier(0);
      _invoker = invoker;
    }

    internal override InternalUpdateByOperation MakeInternal() {
      using var revNative = _revTime.Materialize();
      using var fwdNative = _fwdTime.Materialize();
      _invoker(_timestampCol, _weightCol, _cols, _cols.Length, revNative.Self,
        fwdNative.Self, out var result, out var status);
      status.OkOrThrow();
      return new InternalUpdateByOperation(result);
    }
  }

  public static UpdateByOperation CumSum(string[] cols) =>
    new WithCols(cols, NativeUpdateByOperation.deephaven_client_update_by_cumSum);
  public static UpdateByOperation CumProd(string[] cols) =>
    new WithCols(cols, NativeUpdateByOperation.deephaven_client_update_by_cumProd);
  public static UpdateByOperation CumMin(string[] cols) =>
    new WithCols(cols, NativeUpdateByOperation.deephaven_client_update_by_cumMin);
  public static UpdateByOperation CumMax(string[] cols) =>
    new WithCols(cols, NativeUpdateByOperation.deephaven_client_update_by_cumMin);
  public static UpdateByOperation ForwardFill(string[] cols) =>
    new WithCols(cols, NativeUpdateByOperation.deephaven_client_update_by_forwardFill);

  public static UpdateByOperation Delta(string[] cols, DeltaControl deltaControl = DeltaControl.NullDominates) =>
    new WithDelta(cols, deltaControl,
      NativeUpdateByOperation.deephaven_client_update_by_delta);

  public static UpdateByOperation EmaTick(double decayTicks, string[] cols, OperationControl? opControl = null) =>
    new WithTicks(decayTicks, cols, opControl,
      NativeUpdateByOperation.deephaven_client_update_by_emaTick);
  public static UpdateByOperation EmaTime(string timestampCol, DurationSpecifier decayTime, string[] cols, OperationControl? opControl = null) =>
    new WithTime(timestampCol, decayTime, cols, opControl,
      NativeUpdateByOperation.deephaven_client_update_by_emaTime);
  public static UpdateByOperation EmsTick(double decayTicks, string[] cols, OperationControl? opControl = null) =>
    new WithTicks(decayTicks, cols, opControl,
      NativeUpdateByOperation.deephaven_client_update_by_emsTick);
  public static UpdateByOperation EmsTime(string timestampCol, DurationSpecifier decayTime, string[] cols, OperationControl? opControl = null) =>
    new WithTime(timestampCol, decayTime, cols, opControl,
      NativeUpdateByOperation.deephaven_client_update_by_emsTime);
  public static UpdateByOperation EmminTick(double decayTicks, string[] cols, OperationControl? opControl = null) =>
    new WithTicks(decayTicks, cols, opControl,
      NativeUpdateByOperation.deephaven_client_update_by_emminTick);
  public static UpdateByOperation EmminTime(string timestampCol, DurationSpecifier decayTime, string[] cols, OperationControl? opControl = null) =>
    new WithTime(timestampCol, decayTime, cols, opControl,
      NativeUpdateByOperation.deephaven_client_update_by_emminTime);
  public static UpdateByOperation EmmaxTick(double decayTicks, string[] cols, OperationControl? opControl = null) =>
    new WithTicks(decayTicks, cols, opControl,
      NativeUpdateByOperation.deephaven_client_update_by_emmaxTick);
  public static UpdateByOperation EmmaxTime(string timestampCol, DurationSpecifier decayTime, string[] cols, OperationControl? opControl = null) =>
    new WithTime(timestampCol, decayTime, cols, opControl,
      NativeUpdateByOperation.deephaven_client_update_by_emmaxTime);
  public static UpdateByOperation EmstdTick(double decayTicks, string[] cols, OperationControl? opControl = null) =>
    new WithTicks(decayTicks, cols, opControl,
      NativeUpdateByOperation.deephaven_client_update_by_emstdTick);
  public static UpdateByOperation EmstdTime(string timestampCol, DurationSpecifier decayTime, string[] cols, OperationControl? opControl = null) =>
    new WithTime(timestampCol, decayTime, cols, opControl,
      NativeUpdateByOperation.deephaven_client_update_by_emstdTime);

  public static UpdateByOperation RollingSumTick(string[] cols, Int32 revTicks, Int32 fwdTicks = 0) =>
    new WithRollingTicks(cols, revTicks, fwdTicks,
      NativeUpdateByOperation.deephaven_client_update_by_rollingSumTick);
  public static UpdateByOperation RollingSumTime(string timestampCol, string[] cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) =>
    new WithRollingTime(timestampCol, cols, revTime, fwdTime,
      NativeUpdateByOperation.deephaven_client_update_by_rollingSumTime);
  public static UpdateByOperation RollingGroupTick(string[] cols, Int32 revTicks, Int32 fwdTicks = 0) =>
    new WithRollingTicks(cols, revTicks, fwdTicks,
      NativeUpdateByOperation.deephaven_client_update_by_rollingGroupTick);
  public static UpdateByOperation RollingGroupTime(string timestampCol, string[] cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) =>
    new WithRollingTime(timestampCol, cols, revTime, fwdTime,
      NativeUpdateByOperation.deephaven_client_update_by_rollingGroupTime);
  public static UpdateByOperation RollingAvgTick(string[] cols, Int32 revTicks, Int32 fwdTicks = 0) =>
    new WithRollingTicks(cols, revTicks, fwdTicks,
      NativeUpdateByOperation.deephaven_client_update_by_rollingAvgTick);
  public static UpdateByOperation RollingAvgTime(string timestampCol, string[] cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) =>
    new WithRollingTime(timestampCol, cols, revTime, fwdTime,
      NativeUpdateByOperation.deephaven_client_update_by_rollingAvgTime);
  public static UpdateByOperation RollingMinTick(string[] cols, Int32 revTicks, Int32 fwdTicks = 0) =>
    new WithRollingTicks(cols, revTicks, fwdTicks,
      NativeUpdateByOperation.deephaven_client_update_by_rollingMinTick);
  public static UpdateByOperation RollingMinTime(string timestampCol, string[] cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) =>
    new WithRollingTime(timestampCol, cols, revTime, fwdTime,
      NativeUpdateByOperation.deephaven_client_update_by_rollingMinTime);
  public static UpdateByOperation RollingMaxTick(string[] cols, Int32 revTicks, Int32 fwdTicks = 0) =>
    new WithRollingTicks(cols, revTicks, fwdTicks,
      NativeUpdateByOperation.deephaven_client_update_by_rollingMaxTick);
  public static UpdateByOperation RollingMaxTime(string timestampCol, string[] cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) =>
    new WithRollingTime(timestampCol, cols, revTime, fwdTime,
      NativeUpdateByOperation.deephaven_client_update_by_rollingMaxTime);
  public static UpdateByOperation RollingProdTick(string[] cols, Int32 revTicks, Int32 fwdTicks = 0) =>
    new WithRollingTicks(cols, revTicks, fwdTicks,
      NativeUpdateByOperation.deephaven_client_update_by_rollingProdTick);
  public static UpdateByOperation RollingProdTime(string timestampCol, string[] cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) =>
    new WithRollingTime(timestampCol, cols, revTime, fwdTime,
      NativeUpdateByOperation.deephaven_client_update_by_rollingProdTime);
  public static UpdateByOperation RollingCountTick(string[] cols, Int32 revTicks, Int32 fwdTicks = 0) =>
    new WithRollingTicks(cols, revTicks, fwdTicks,
      NativeUpdateByOperation.deephaven_client_update_by_rollingCountTick);
  public static UpdateByOperation RollingCountTime(string timestampCol, string[] cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) =>
    new WithRollingTime(timestampCol, cols, revTime, fwdTime,
      NativeUpdateByOperation.deephaven_client_update_by_rollingCountTime);
  public static UpdateByOperation RollingStdTick(string[] cols, Int32 revTicks, Int32 fwdTicks = 0) =>
    new WithRollingTicks(cols, revTicks, fwdTicks,
      NativeUpdateByOperation.deephaven_client_update_by_rollingStdTick);
  public static UpdateByOperation RollingStdTime(string timestampCol, string[] cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) =>
    new WithRollingTime(timestampCol, cols, revTime, fwdTime,
      NativeUpdateByOperation.deephaven_client_update_by_rollingStdTime);

  public static UpdateByOperation RollingWavgTick(string weightCol, string[] cols,
    Int32 revTicks, Int32 fwdTicks = 0) =>
      new WithRollingWavgTicks(weightCol, cols, revTicks, fwdTicks,
      NativeUpdateByOperation.deephaven_client_update_by_rollingWavgTick);
  public static UpdateByOperation RollingWavgTime(string timestampCol, string weightCol, string[] cols,
    DurationSpecifier revTime, DurationSpecifier? fwdTime = null) =>
    new WithRollingWavgTime(timestampCol, weightCol, cols, revTime, fwdTime,
      NativeUpdateByOperation.deephaven_client_update_by_rollingWavgTime);
}

internal class InternalUpdateByOperation : IDisposable {
  internal NativePtr<NativeUpdateByOperation> Self;

  internal InternalUpdateByOperation(NativePtr<NativeUpdateByOperation> self) => Self = self;

  ~InternalUpdateByOperation() {
    ReleaseUnmanagedResources();
  }

  public void Dispose() {
    ReleaseUnmanagedResources();
    GC.SuppressFinalize(this);
  }

  private void ReleaseUnmanagedResources() {
    if (!Self.TryRelease(out var old)) {
      return;
    }
    NativeUpdateByOperation.deephaven_client_UpdateByOperation_dtor(old);
  }
}

internal partial class NativeUpdateByOperation {
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_UpdateByOperation_dtor(NativePtr<NativeUpdateByOperation> self);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_cumSum(string[] cols, Int32 numCols,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_cumProd(string[] cols, Int32 numCols,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_cumMin(string[] cols, Int32 numCols,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_cumMax(string[] cols, Int32 numCols,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_forwardFill(string[] cols, Int32 numCols,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_delta(string[] cols, Int32 numCols,
    DeltaControl deltaControl, out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_emaTick(double decayTicks,
    string[] cols, Int32 numCols, ref OperationControl opControl,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_emaTime(string timestampCol,
    NativePtr<NativeDurationSpecifier> decayTime, string[] cols, Int32 numCols,
  ref OperationControl opControl, out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_emsTick(double decayTicks,
    string[] cols, Int32 numCols, ref OperationControl opControl,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_emsTime(string timestampCol,
    NativePtr<NativeDurationSpecifier> decayTime, string[] cols, Int32 numCols,
    ref OperationControl opControl, out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_emminTick(double decayTicks,
    string[] cols, Int32 numCols, ref OperationControl opControl,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_emminTime(string timestampCol,
    NativePtr<NativeDurationSpecifier> decayTime, string[] cols, Int32 numCols,
    ref OperationControl opControl, out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_emmaxTick(double decayTicks,
    string[] cols, Int32 numCols, ref OperationControl opControl,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_emmaxTime(string timestampCol,
    NativePtr<NativeDurationSpecifier> decayTime, string[] cols, Int32 numCols,
    ref OperationControl opControl, out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_emstdTick(double decayTicks,
    string[] cols, Int32 numCols, ref OperationControl opControl,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_emstdTime(string timestampCol,
    NativePtr<NativeDurationSpecifier> decayTime, string[] cols, Int32 numCols,
    ref OperationControl opControl, out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingSumTick(
    string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingSumTime(string timestampCol,
    string[] cols, Int32 numCols, NativePtr<NativeDurationSpecifier> revTime,
    NativePtr<NativeDurationSpecifier> fwdTime,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingGroupTick(
    string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingGroupTime(string timestampCol,
    string[] cols, Int32 numCols, NativePtr<NativeDurationSpecifier> revTime,
    NativePtr<NativeDurationSpecifier> fwdTime,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingAvgTick(
    string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingAvgTime(string timestampCol,
    string[] cols, Int32 numCols, NativePtr<NativeDurationSpecifier> revTime,
    NativePtr<NativeDurationSpecifier> fwdTime,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingMinTick(
    string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingMinTime(string timestampCol,
    string[] cols, Int32 numCols, NativePtr<NativeDurationSpecifier> revTime,
    NativePtr<NativeDurationSpecifier> fwdTime,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingMaxTick(
    string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingMaxTime(string timestampCol,
    string[] cols, Int32 numCols, NativePtr<NativeDurationSpecifier> revTime,
    NativePtr<NativeDurationSpecifier> fwdTime,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingProdTick(
    string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingProdTime(string timestampCol,
    string[] cols, Int32 numCols, NativePtr<NativeDurationSpecifier> revTime,
    NativePtr<NativeDurationSpecifier> fwdTime,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingCountTick(
    string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingCountTime(string timestampCol,
    string[] cols, Int32 numCols, NativePtr<NativeDurationSpecifier> revTime,
    NativePtr<NativeDurationSpecifier> fwdTime,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingStdTick(
    string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingStdTime(string timestampCol,
    string[] cols, Int32 numCols, NativePtr<NativeDurationSpecifier> revTime,
    NativePtr<NativeDurationSpecifier> fwdTime,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingWavgTick(
    string weightCol, string[] cols, Int32 numCols, Int32 revTicks, Int32 fwdTicks,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
  [LibraryImport(LibraryPaths.Dhclient, StringMarshalling = StringMarshalling.Utf8)]
  public static partial void deephaven_client_update_by_rollingWavgTime(string timestampCol,
    string weightCol, string[] cols, Int32 numCols, NativePtr<NativeDurationSpecifier> revTime,
    NativePtr<NativeDurationSpecifier> fwdTime,
    out NativePtr<NativeUpdateByOperation> result, out ErrorStatus status);
}
