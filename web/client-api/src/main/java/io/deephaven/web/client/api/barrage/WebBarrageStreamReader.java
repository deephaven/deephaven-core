//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import com.google.common.io.LittleEndianDataInputStream;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageModColumnMetadata;
import io.deephaven.barrage.flatbuf.BarrageUpdateMetadata;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.chunk.ChunkReader;
import io.deephaven.extensions.barrage.util.FlatBufferIteratorAdapter;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.io.streams.ByteBufferInputStream;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flatbuf.Schema;
import org.gwtproject.nio.TypedArrayHelper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;

/**
 * Consumes FlightData fields from Flight/Barrage producers and builds browser-compatible WebBarrageMessage payloads
 * that can be used to maintain table data.
 */
public class WebBarrageStreamReader {
    private static final int MAX_CHUNK_SIZE = Integer.MAX_VALUE - 8;

    // record progress in reading
    private long numAddRowsRead = 0;
    private long numAddRowsTotal = 0;
    private long numModRowsRead = 0;
    private long numModRowsTotal = 0;

    // hold in-progress messages that aren't finished being built
    private WebBarrageMessage msg;

    private final WebChunkReaderFactory chunkReaderFactory = new WebChunkReaderFactory();
    private final List<ChunkReader> readers = new ArrayList<>();

    public WebBarrageMessage parseFrom(
            final StreamReaderOptions options,
            ChunkType[] columnChunkTypes,
            Class<?>[] columnTypes,
            Class<?>[] componentTypes,
            FlightData flightData) throws IOException {
        ByteBuffer headerAsBB = TypedArrayHelper.wrap(flightData.getDataHeader_asU8());
        Message header = headerAsBB.hasRemaining() ? Message.getRootAsMessage(headerAsBB) : null;

        ByteBuffer msgAsBB = TypedArrayHelper.wrap(flightData.getAppMetadata_asU8());
        if (msgAsBB.hasRemaining()) {
            BarrageMessageWrapper wrapper =
                    BarrageMessageWrapper.getRootAsBarrageMessageWrapper(msgAsBB);
            if (wrapper.magic() != WebBarrageUtils.FLATBUFFER_MAGIC) {
                JsLog.warn(
                        "WebBarrageStreamReader: skipping app_metadata that does not look like BarrageMessageWrapper");
            } else if (wrapper.msgType() == BarrageMessageType.BarrageUpdateMetadata) {
                if (msg != null) {
                    throw new IllegalStateException(
                            "Previous message was not complete; pending " + (numAddRowsTotal - numAddRowsRead)
                                    + " add rows and " + (numModRowsTotal - numModRowsRead) + " mod rows");
                }

                final BarrageUpdateMetadata metadata =
                        BarrageUpdateMetadata.getRootAsBarrageUpdateMetadata(wrapper.msgPayloadAsByteBuffer());

                msg = new WebBarrageMessage();

                msg.isSnapshot = metadata.isSnapshot();
                msg.snapshotRowSetIsReversed = metadata.effectiveReverseViewport();

                numAddRowsRead = 0;
                numModRowsRead = 0;

                if (msg.isSnapshot) {
                    final ByteBuffer effectiveViewport = metadata.effectiveViewportAsByteBuffer();
                    if (effectiveViewport != null) {
                        msg.snapshotRowSet = extractIndex(effectiveViewport);
                    }
                    final ByteBuffer effectiveSnapshotColumns = metadata.effectiveColumnSetAsByteBuffer();
                    if (effectiveSnapshotColumns != null) {
                        msg.snapshotColumns = extractBitSet(effectiveSnapshotColumns);
                    }
                }

                msg.firstSeq = metadata.firstSeq();
                msg.lastSeq = metadata.lastSeq();
                msg.tableSize = metadata.tableSize();
                msg.rowsAdded = extractIndex(metadata.addedRowsAsByteBuffer());
                msg.rowsRemoved = extractIndex(metadata.removedRowsAsByteBuffer());
                final ByteBuffer shiftData = metadata.shiftDataAsByteBuffer();
                msg.shifted = shiftData != null ? extractIndexShiftData(shiftData) : new ShiftedRange[0];

                final ByteBuffer rowsIncluded = metadata.addedRowsIncludedAsByteBuffer();
                msg.rowsIncluded = rowsIncluded != null ? extractIndex(rowsIncluded) : msg.rowsAdded;
                msg.addColumnData = new WebBarrageMessage.AddColumnData[columnTypes.length];
                for (int ci = 0; ci < msg.addColumnData.length; ++ci) {
                    msg.addColumnData[ci] = new WebBarrageMessage.AddColumnData();
                    // msg.addColumnData[ci].type = columnTypes[ci];
                    // msg.addColumnData[ci].componentType = componentTypes[ci];
                    msg.addColumnData[ci].data = new ArrayList<>();

                    // create an initial chunk of the correct size
                    final int chunkSize = (int) (Math.min(msg.rowsIncluded.size(), MAX_CHUNK_SIZE));
                    final WritableChunk<Values> chunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);
                    chunk.setSize(0);
                    msg.addColumnData[ci].data.add(chunk);
                }
                numAddRowsTotal = msg.rowsIncluded.size();

                // if this message is a snapshot response (vs. subscription) then mod columns may be empty
                numModRowsTotal = 0;
                msg.modColumnData = new WebBarrageMessage.ModColumnData[metadata.modColumnNodesLength()];
                for (int ci = 0; ci < msg.modColumnData.length; ++ci) {
                    msg.modColumnData[ci] = new WebBarrageMessage.ModColumnData();
                    // msg.modColumnData[ci].type = columnTypes[ci];
                    // msg.modColumnData[ci].componentType = componentTypes[ci];
                    msg.modColumnData[ci].data = new ArrayList<>();

                    final BarrageModColumnMetadata mcd = metadata.modColumnNodes(ci);
                    msg.modColumnData[ci].rowsModified = extractIndex(mcd.modifiedRowsAsByteBuffer());

                    // create an initial chunk of the correct size
                    final int chunkSize = (int) (Math.min(msg.modColumnData[ci].rowsModified.size(),
                            MAX_CHUNK_SIZE));
                    final WritableChunk<Values> chunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);
                    chunk.setSize(0);
                    msg.modColumnData[ci].data.add(chunk);

                    numModRowsTotal = Math.max(numModRowsTotal, msg.modColumnData[ci].rowsModified.size());
                }
            }
        }

        byte headerType = header.headerType();
        if (headerType == MessageHeader.Schema) {
            // there is no body and our clients do not want to see schema messages
            Schema schema = new Schema();
            header.header(schema);
            for (int i = 0; i < schema.fieldsLength(); i++) {
                Field field = schema.fields(i);
                ChunkReader chunkReader = chunkReaderFactory.getReader(options,
                        ChunkReader.typeInfo(columnChunkTypes[i], columnTypes[i],
                                componentTypes[i], field));
                readers.add(chunkReader);
            }
            return null;
        }
        if (headerType != MessageHeader.RecordBatch) {
            throw new IllegalStateException("Only know how to decode Schema/RecordBatch messages");
        }


        // throw an error when no app metadata (snapshots now provide by default)
        if (msg == null) {
            throw new IllegalStateException(
                    "Missing app metadata tag; cannot decode using BarrageStreamReader");
        }

        final RecordBatch batch = (RecordBatch) header.header(new RecordBatch());
        msg.length = batch.length();
        ByteBuffer body = TypedArrayHelper.wrap(flightData.getDataBody_asU8());
        final LittleEndianDataInputStream ois =
                new LittleEndianDataInputStream(new ByteBufferInputStream(body));
        final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                        i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

        final long[] bufferInfo = new long[batch.buffersLength()];
        for (int bi = 0; bi < batch.buffersLength(); ++bi) {
            int offset = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).offset());
            int length = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).length());
            if (bi < batch.buffersLength() - 1) {
                final int nextOffset =
                        LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi + 1).offset());
                // our parsers handle overhanging buffers
                length += Math.max(0, nextOffset - offset - length);
            }
            bufferInfo[bi] = length;
        }
        final PrimitiveIterator.OfLong bufferInfoIter = Arrays.stream(bufferInfo).iterator();


        // add and mod rows are never combined in a batch. all added rows must be received before the first
        // mod rows will be received.
        if (numAddRowsRead < numAddRowsTotal) {
            for (int ci = 0; ci < msg.addColumnData.length; ++ci) {
                final WebBarrageMessage.AddColumnData acd = msg.addColumnData[ci];

                final long remaining = numAddRowsTotal - numAddRowsRead;
                if (batch.length() > remaining) {
                    throw new IllegalStateException(
                            "Batch length exceeded the expected number of rows from app metadata");
                }

                // select the current chunk size and read the size
                int lastChunkIndex = acd.data.size() - 1;
                WritableChunk<Values> chunk = (WritableChunk<Values>) acd.data.get(lastChunkIndex);

                if (batch.length() > chunk.capacity() - chunk.size()) {
                    // reading the rows from this batch will overflow the existing chunk; create a new one
                    final int chunkSize = (int) (Math.min(remaining, MAX_CHUNK_SIZE));
                    chunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);
                    acd.data.add(chunk);

                    chunk.setSize(0);
                    ++lastChunkIndex;
                }

                // fill the chunk with data and assign back into the array
                acd.data.set(lastChunkIndex,
                        readers.get(ci).readChunk(fieldNodeIter, bufferInfoIter, ois, chunk, chunk.size(),
                                (int) batch.length()));
                chunk.setSize(chunk.size() + (int) batch.length());
            }
            numAddRowsRead += batch.length();
        } else {
            for (int ci = 0; ci < msg.modColumnData.length; ++ci) {
                final WebBarrageMessage.ModColumnData mcd = msg.modColumnData[ci];

                // another column may be larger than this column
                long remaining = Math.max(0, mcd.rowsModified.size() - numModRowsRead);

                // need to add the batch row data to the column chunks
                int lastChunkIndex = mcd.data.size() - 1;
                WritableChunk<Values> chunk = (WritableChunk<Values>) mcd.data.get(lastChunkIndex);

                final int numRowsToRead = LongSizedDataStructure.intSize("BarrageStreamReader",
                        Math.min(remaining, batch.length()));
                if (numRowsToRead > chunk.capacity() - chunk.size()) {
                    // reading the rows from this batch will overflow the existing chunk; create a new one
                    final int chunkSize = (int) (Math.min(remaining, MAX_CHUNK_SIZE));
                    chunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);
                    mcd.data.add(chunk);

                    chunk.setSize(0);
                    ++lastChunkIndex;
                }

                // fill the chunk with data and assign back into the array
                mcd.data.set(lastChunkIndex,
                        readers.get(ci).readChunk(fieldNodeIter, bufferInfoIter, ois, chunk, chunk.size(),
                                numRowsToRead));
                chunk.setSize(chunk.size() + numRowsToRead);
            }
            numModRowsRead += batch.length();
        }

        if (numAddRowsRead == numAddRowsTotal && numModRowsRead == numModRowsTotal) {
            final WebBarrageMessage retval = msg;
            msg = null;
            return retval;
        }

        // otherwise, must wait for more data
        return null;
    }

    private static RangeSet extractIndex(final ByteBuffer bb) {
        if (bb == null) {
            return RangeSet.empty();
        }
        return new CompressedRangeSetReader().read(bb);
    }

    private static BitSet extractBitSet(final ByteBuffer bb) {
        byte[] array = new byte[bb.remaining()];
        bb.get(array);
        return BitSet.valueOf(array);
    }

    private static ShiftedRange[] extractIndexShiftData(final ByteBuffer bb) {
        return ShiftedRangeReader.read(bb);
    }
}
