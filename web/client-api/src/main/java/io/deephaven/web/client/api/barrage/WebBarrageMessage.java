//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;

import java.util.ArrayList;
import java.util.BitSet;

public class WebBarrageMessage {
    public static class ModColumnData {
        public RangeSet rowsModified;
        public Class<?> type;
        public Class<?> componentType;
        public ArrayList<Chunk<Values>> data;
        public ChunkType chunkType;
    }
    public static class AddColumnData {
        public Class<?> type;
        public Class<?> componentType;
        public ArrayList<Chunk<Values>> data;
        public ChunkType chunkType;
    }

    public long firstSeq = -1;
    public long lastSeq = -1;
    public long step = -1;
    public long tableSize = -1;

    public boolean isSnapshot;
    public RangeSet snapshotRowSet;
    public boolean snapshotRowSetIsReversed;
    public BitSet snapshotColumns;

    public RangeSet rowsAdded;
    public RangeSet rowsIncluded;
    public RangeSet rowsRemoved;
    public ShiftedRange[] shifted;

    public AddColumnData[] addColumnData;
    public ModColumnData[] modColumnData;

    // Underlying RecordBatch.length, visible for reading snapshots
    public long length;
}
