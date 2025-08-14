//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//

using Apache.Arrow;
using Google.FlatBuffers;
using io.deephaven.barrage.flatbuf;
using Array = System.Array;

namespace Deephaven.Dh_NetClient;

interface IChunkProcessor {
  (TickingUpdate?, IChunkProcessor) ProcessNextChunk(SourceAndRange[] sourcesAndRanges, byte[]? metadata);
}

public class BarrageProcessor {
  private IChunkProcessor _currentProcessor;

  public BarrageProcessor(Schema schema) {
    var tableState = new TableState(schema);
    _currentProcessor = new AwaitingMetadata(tableState);
  }

  // "dphn"u8.ToArray();
  public const UInt32 DeephavenMagicNumber = 0x6E687064U;

  public static byte[] CreateSubscriptionRequest(byte[] ticketBytes) {
    var payloadBuilder = new FlatBufferBuilder(4096);

    var subOptions = BarrageSubscriptionOptions.CreateBarrageSubscriptionOptions(
      payloadBuilder, ColumnConversionMode.Stringify, true, 0, 4096, 0, true);

    // add ticket
    payloadBuilder.StartVector(1, ticketBytes.Length, 1);
    payloadBuilder.Add(ticketBytes);
    var ticket = payloadBuilder.EndVector();

    var subReq = BarrageSubscriptionRequest.CreateBarrageSubscriptionRequest(payloadBuilder, ticket, default,
      default, subOptions);
    payloadBuilder.Finish(subReq.Value);

    var payloadBuilderBytes = payloadBuilder.SizedByteArray();

    // Now that we've constructed the bytes for the payload, let's build the outer BarrageMessageWrapper
    // which has payload as one of its members.
    var wrapperBuilder = new FlatBufferBuilder(4096);
    wrapperBuilder.StartVector(1, payloadBuilderBytes.Length, 1);
    wrapperBuilder.Add(payloadBuilderBytes);
    var payload = wrapperBuilder.EndVector();
    var messageWrapper = BarrageMessageWrapper.CreateBarrageMessageWrapper(wrapperBuilder,
      DeephavenMagicNumber, BarrageMessageType.BarrageSubscriptionRequest, payload);
    wrapperBuilder.Finish(messageWrapper.Value);

    return wrapperBuilder.SizedByteArray();
  }

  public TickingUpdate? ProcessNextChunk(IColumnSource[] sources, int[] sizes, byte[]? metadata) {
    if (sources.Length != sizes.Length) {
      throw new Exception($"Assertion failed: sources has length {sources.Length} but sizes has length {sizes.Length}");
    }
    var sourcesAndRanges = Enumerable.Range(0, sources.Length)
      .Select(i => new SourceAndRange(sources[i], Interval.Of(0, (UInt64)sizes[i])))
      .ToArray();
    var (update, next) = _currentProcessor.ProcessNextChunk(sourcesAndRanges, metadata);
    _currentProcessor = next;
    return update;
  }
}

class AwaitingMetadata(TableState tableState) : IChunkProcessor {
  public (TickingUpdate?, IChunkProcessor) ProcessNextChunk(SourceAndRange[] sourcesAndRanges, byte[]? metadata) {
    if (metadata == null) {
      throw new Exception("Metadata was required here, but none was received");
    }

    var bmw = BarrageMessageWrapper.GetRootAsBarrageMessageWrapper(new ByteBuffer(metadata));

    if (bmw.Magic != BarrageProcessor.DeephavenMagicNumber) {
      throw new Exception($"Expected magic number {BarrageProcessor.DeephavenMagicNumber:x}, got {bmw.Magic:x}");
    }

    if (bmw.MsgType != BarrageMessageType.BarrageUpdateMetadata) {
      throw new Exception(
        $"Expected Barrage Message Type {BarrageMessageType.BarrageUpdateMetadata}, got {bmw.MsgType}");
    }

    var bytes = bmw.GetMsgPayloadBytes()!.ToArray<byte>();
    var bmd = BarrageUpdateMetadata.GetRootAsBarrageUpdateMetadata(new ByteBuffer(bytes));

    var removedRowsBytes = bmd.GetRemovedRowsBytes();
    var shiftDataBytes = bmd.GetShiftDataBytes();
    var addedRowsBytes = bmd.GetAddedRowsBytes();
    if (removedRowsBytes == null || shiftDataBytes == null || addedRowsBytes == null) {
      throw new Exception("Assertion failed: These data structures should not be null");
    }

    var diRemoved = new DataInput(removedRowsBytes);
    var diThreeShiftIndices = new DataInput(shiftDataBytes);
    var diAdded = new DataInput(addedRowsBytes);

    var removedRows = RowSequenceDecoder.ReadExternalCompressedDelta(diRemoved);
    var shiftStartIndex = RowSequenceDecoder.ReadExternalCompressedDelta(diThreeShiftIndices);
    var shiftEndIndex = RowSequenceDecoder.ReadExternalCompressedDelta(diThreeShiftIndices);
    var shiftDestIndex = RowSequenceDecoder.ReadExternalCompressedDelta(diThreeShiftIndices);
    var addedRows = RowSequenceDecoder.ReadExternalCompressedDelta(diAdded);

    var perColumnModifies = new List<RowSequence>();
    for (var i = 0; i != bmd.ModColumnNodesLength; ++i) {
      var mcns = bmd.ModColumnNodes(i);
      if (!mcns.HasValue) {
        throw new Exception($"Assertion failed: ModColumnNodes[{i}] should not be empty");
      }

      var modifiedRowsBytes = mcns.Value.GetModifiedRowsBytes();
      if (modifiedRowsBytes == null) {
        throw new Exception($"Assertion failed: modifiedRowsBytes[{i}] should not be null");
      }

      var diModified = new DataInput(modifiedRowsBytes);
      var modRows = RowSequenceDecoder.ReadExternalCompressedDelta(diModified);
      perColumnModifies.Add(modRows);
    }

    // Correct order to process Barrage info is:
    // 1. removes
    // 2. shifts
    // 3. adds
    // 4. modifies
    // We have not called with add or modify data yet, but we can do removes and shifts now
    // (steps 1 and 2).
    var (prev, removedRowsIndexSpace, afterRemoves) = ProcessRemoves(removedRows);
    tableState.ApplyShifts(shiftStartIndex, shiftEndIndex, shiftDestIndex);

    var addedRowsIndexSpace = tableState.AddKeys(addedRows);

    var nextState = new AwaitingAdds(tableState, perColumnModifies.ToArray(), prev, removedRowsIndexSpace, afterRemoves,
      addedRowsIndexSpace);
    return nextState.ProcessNextChunk(sourcesAndRanges, Array.Empty<byte>());
  }

  (IClientTable, RowSequence, IClientTable) ProcessRemoves(RowSequence removedRows) {
    var prev = tableState.Snapshot();
    // The reason we special-case "empty" is because when the tables are unchanged, we prefer
    // to indicate this via pointer equality (e.g. beforeRemoves == afterRemoves).
    RowSequence removedRowsIndexSpace;
    IClientTable afterRemoves;
    if (removedRows.IsEmpty) {
      removedRowsIndexSpace = RowSequence.CreateEmpty();
      afterRemoves = prev;
    } else {
      removedRowsIndexSpace = tableState.Erase(removedRows);
      afterRemoves = tableState.Snapshot();
    }

    return (prev, removedRowsIndexSpace, afterRemoves);
  }
}

class AwaitingAdds(
  TableState tableState,
  RowSequence[] perColumnModifies,
  IClientTable prev,
  RowSequence removedRowsIndexSpace,
  IClientTable afterRemoves,
  RowSequence addedRowsIndexSpace) : IChunkProcessor {

  bool _firstTime = true;
  private RowSequence _addedRowsRemaining = RowSequence.CreateEmpty();

  public (TickingUpdate?, IChunkProcessor) ProcessNextChunk(SourceAndRange[] sourcesAndRanges, byte[]? metadata) {
    var numCols = sourcesAndRanges.Length;
    if (_firstTime) {
      _firstTime = false;

      if (addedRowsIndexSpace.IsEmpty) {
        _addedRowsRemaining = RowSequence.CreateEmpty();

        var afterAdds = afterRemoves;
        var nextState = new AwaitingModifies(tableState, prev, removedRowsIndexSpace, afterRemoves,
          addedRowsIndexSpace, afterAdds, perColumnModifies);
        return nextState.ProcessNextChunk(sourcesAndRanges, metadata);
      }

      if (numCols == 0) {
        throw new Exception("AddedRows is not empty but numCols == 0");
      }

      // Working copy that is consumed in the iterations of the loop.
      _addedRowsRemaining = addedRowsIndexSpace;
    }

    if (_addedRowsRemaining.IsEmpty) {
      throw new Exception("Assertion failed: addedRowsRemaining is empty");
    }

    var chunkSize = sourcesAndRanges[0].Range.Count;
    if (sourcesAndRanges.Any(sar => sar.Range.Count != chunkSize)) {
      throw new Exception($"Chunks have inconsistent sizes: [{string.Join(",", sourcesAndRanges.Select(sar => sar.Range.Count))}]");
    }

    if (chunkSize == 0) {
      // Need more data from caller.
      return (null, this);
    }

    if (_addedRowsRemaining.Count < chunkSize) {
      throw new Exception($"There is excess data in the chunk that I won't be able to process. Expected {_addedRowsRemaining.Count}, have {chunkSize}");
    }

    var indexRowsThisTime = _addedRowsRemaining.Take(chunkSize);
    _addedRowsRemaining = _addedRowsRemaining.Drop(chunkSize);
    tableState.AddData(sourcesAndRanges, indexRowsThisTime);

    // To indicate to the next stages in the pipeline that we've consumed all the data here.
    for (var i = 0; i != sourcesAndRanges.Length; ++i) {
      sourcesAndRanges[i] = sourcesAndRanges[i] with { Range = Interval.OfEmpty };
    }

    if (!_addedRowsRemaining.IsEmpty) {
      // Need more data from caller.
      return (null, this);
    }

    // No more data remaining. The Add phase is done.
    {
      var afterAdds = tableState.Snapshot();
      var nextState = new AwaitingModifies(tableState, prev, removedRowsIndexSpace, afterRemoves,
        addedRowsIndexSpace, afterAdds, perColumnModifies);
      return nextState.ProcessNextChunk(sourcesAndRanges, metadata);
    }
  }
}

class AwaitingModifies(
  TableState tableState,
  IClientTable prev,
  RowSequence removedRowsIndexSpace,
  IClientTable afterRemoves,
  RowSequence addedRowsIndexSpace,
  IClientTable afterAdds,
  RowSequence[] perColumnModifies) : IChunkProcessor {
  private bool _firstTime = true;
  private RowSequence[] _modifiedRowsRemaining = [];
  private RowSequence[] _modifiedRowsIndexSpace = [];

  public (TickingUpdate?, IChunkProcessor) ProcessNextChunk(SourceAndRange[] sourcesAndRanges, byte[]? metadata) {
    var numCols = sourcesAndRanges.Length;
    if (_firstTime) {
      _firstTime = false;

      if (perColumnModifies.All(rs => rs.IsEmpty)) {
        var afterModifies = afterAdds;
        var nextState = new BuildingResult(tableState, prev, removedRowsIndexSpace, afterRemoves,
          addedRowsIndexSpace, afterAdds, _modifiedRowsIndexSpace, afterModifies);
        return nextState.ProcessNextChunk(sourcesAndRanges, metadata);
      }

      _modifiedRowsIndexSpace = new RowSequence[numCols];
      _modifiedRowsRemaining = new RowSequence[numCols];
      for (var i = 0; i < numCols; ++i) {
        var rs = tableState.ConvertKeysToIndices(perColumnModifies[i]);
        _modifiedRowsIndexSpace[i] = rs;
        _modifiedRowsRemaining[i] = rs;
      }
    }

    if (_modifiedRowsRemaining.All(rs => rs.IsEmpty)) {
      throw new Exception("Impossible: modifiedRowsRemaining is empty");
    }

    if (sourcesAndRanges.All(sar => sar.Range.IsEmpty)) {
      // Need more data from caller.
      return (null, this);
    }

    for (var i = 0; i < numCols; ++i) {
      var numRowsRemaining = _modifiedRowsRemaining[i].Count;
      var numRowsAvailable = sourcesAndRanges[i].Range.Count;

      if (numRowsAvailable > numRowsRemaining) {
        throw new Exception($"col {i}: numRowsAvailable ({numRowsAvailable}) > numRowsRemaining ({numRowsRemaining})");
      }

      if (numRowsAvailable == 0) {
        // Nothing available for this column. Advance to next column.
        continue;
      }

      var mr = _modifiedRowsRemaining[i];
      var rowsAvailable = mr.Take(numRowsAvailable);
      _modifiedRowsRemaining[i] = mr.Drop(numRowsAvailable);

      tableState.ModifyData(i, sourcesAndRanges[i], rowsAvailable);
      sourcesAndRanges[i] = sourcesAndRanges[i] with { Range = Interval.OfEmpty };
    }

    if (_modifiedRowsRemaining.Any(mr => !mr.IsEmpty)) {
      // At least one of our columns is hungry for more data that we don't have.
      return (null, this);
    }

    {
      var afterModifies = tableState.Snapshot();
      var nextState = new BuildingResult(tableState, prev, removedRowsIndexSpace, afterRemoves,
        addedRowsIndexSpace, afterAdds, _modifiedRowsIndexSpace, afterModifies);
      return nextState.ProcessNextChunk(sourcesAndRanges, metadata);
    }
  }
}

class BuildingResult(
  TableState tableState,
  IClientTable prev,
  RowSequence removedRowsIndexSpace,
  IClientTable afterRemoves,
  RowSequence addedRowsIndexSpace,
  IClientTable afterAdds,
  RowSequence[] modifiedRowsIndexSpace,
  IClientTable afterModifies) : IChunkProcessor {

  public (TickingUpdate?, IChunkProcessor) ProcessNextChunk(SourceAndRange[] sourcesAndRanges, byte[]? metadata) {
    if (sourcesAndRanges.Any(sar => !sar.Range.IsEmpty)) {
      throw new Exception(
        $"Barrage logic is done processing but there is leftover caller-provided data: " +
        $"[{string.Join(", ", sourcesAndRanges.Select(sar => sar.Range))}]");
    }

    var result = new TickingUpdate(prev,
      removedRowsIndexSpace, afterRemoves,
      addedRowsIndexSpace, afterAdds,
      modifiedRowsIndexSpace, afterModifies);
    var nextState = new AwaitingMetadata(tableState);
    return (result, nextState);
  }
}
