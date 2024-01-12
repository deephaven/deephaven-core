package io.deephaven.web.client.api.barrage;

import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageModColumnMetadata;
import io.deephaven.barrage.flatbuf.BarrageUpdateMetadata;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.gwtproject.nio.TypedArrayHelper;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;

/**
 * Consumes FlightData fields from Flight/Barrage producers and builds
 * browser-compatible WebBarrageMessage payloads that can be used to
 * maintain table data.
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

    public WebBarrageMessage parseFrom(BitSet expectedColumns, ChunkType[] columnChunkTypes, Class<?>[] columnTypes, Class<?>[] componentTypes,
                                       FlightData flightData) {
        ByteBuffer headerAsBB = TypedArrayHelper.wrap(flightData.getDataHeader_asU8());
        Message header = headerAsBB.hasRemaining() ? Message.getRootAsMessage(headerAsBB) : null;

        ByteBuffer msgAsBB = TypedArrayHelper.wrap(flightData.getAppMetadata_asU8());
        if (msgAsBB.hasRemaining()) {
            BarrageMessageWrapper wrapper =
                    BarrageMessageWrapper.getRootAsBarrageMessageWrapper(msgAsBB);
            if (wrapper.magic() != WebBarrageUtils.FLATBUFFER_MAGIC) {
                //TODO warn
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
                msg.rowsAdded = extractIndex(metadata.addedRowsAsByteBuffer());
                msg.rowsRemoved = extractIndex(metadata.removedRowsAsByteBuffer());
                msg.shifted = extractIndexShiftData(metadata.shiftDataAsByteBuffer());

                final ByteBuffer rowsIncluded = metadata.addedRowsIncludedAsByteBuffer();
                msg.rowsIncluded = rowsIncluded != null ? extractIndex(rowsIncluded) : msg.rowsAdded;
                msg.addColumnData = new WebBarrageMessage.AddColumnData[columnTypes.length];
                for (int ci = 0; ci < msg.addColumnData.length; ++ci) {
                    msg.addColumnData[ci] = new WebBarrageMessage.AddColumnData();
                    msg.addColumnData[ci].type = columnTypes[ci];
                    msg.addColumnData[ci].componentType = componentTypes[ci];
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
                    msg.modColumnData[ci].type = columnTypes[ci];
                    msg.modColumnData[ci].componentType = componentTypes[ci];
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
        ByteBuffer body = TypedArrayHelper.wrap(flightData.getDataBody_asU8());
        if (!body.hasRemaining()) {
            throw new IllegalStateException("Missing body tag");
        }
        if (header == null) {
            throw new IllegalStateException("Missing metadata header; cannot decode body");
        }

        if (header.headerType() != MessageHeader.RecordBatch) {
            throw new IllegalStateException("Only know how to decode Schema/BarrageRecordBatch messages");
        }

        // throw an error when no app metadata (snapshots now provide by default)
        if (msg == null) {
            throw new IllegalStateException(
                    "Missing app metadata tag; cannot decode using BarrageStreamReader");
        }

        final RecordBatch batch = (RecordBatch) header.header(new RecordBatch());
        msg.length = batch.length();

        final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                        i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

        final TLongArrayList bufferInfo = new TLongArrayList(batch.buffersLength());
        for (int bi = 0; bi < batch.buffersLength(); ++bi) {
            int offset = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).offset());
            int length = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).length());
            if (bi < batch.buffersLength() - 1) {
                final int nextOffset =
                        LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi + 1).offset());
                // our parsers handle overhanging buffers
                length += Math.max(0, nextOffset - offset - length);
            }
            bufferInfo.add(length);
        }
        final TLongIterator bufferInfoIter = bufferInfo.iterator();


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
                        ChunkInputStreamGenerator.extractChunkFromInputStream(options, columnChunkTypes[ci],
                                columnTypes[ci], componentTypes[ci], fieldNodeIter, bufferInfoIter, ois,
                                chunk, chunk.size(), (int) batch.length()));
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
                        ChunkInputStreamGenerator.extractChunkFromInputStream(options, columnChunkTypes[ci],
                                columnTypes[ci], componentTypes[ci], fieldNodeIter, bufferInfoIter, ois,
                                chunk, chunk.size(), numRowsToRead));
                chunk.setSize(chunk.size() + numRowsToRead);
            }
            numModRowsRead += batch.length();
        }


        if (header.headerType() == MessageHeader.Schema) {
            // there is no body and our clients do not want to see schema messages
            return null;
        }

        if (numAddRowsRead == numAddRowsTotal && numModRowsRead == numModRowsTotal) {
            final WebBarrageMessage retval = msg;
            msg = null;
            return retval;
        }

        // otherwise, must wait for more data
        return null;
    }

    static WritableChunk<Values> extractChunkFromInputStream(
            final StreamReaderOptions options,
            final ChunkType chunkType, final Class<?> type, final Class<?> componentType,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final TLongIterator bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk, final int offset, final int totalRows) throws IOException {
        return extractChunkFromInputStream(options, 1, chunkType, type, componentType, fieldNodeIter, bufferInfoIter, is,
                outChunk, offset, totalRows);
    }

    static WritableChunk<Values> extractChunkFromInputStream(
            final StreamReaderOptions options,
            final int factor,
            final ChunkType chunkType, final Class<?> type, final Class<?> componentType,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final TLongIterator bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk, final int outOffset, final int totalRows) throws IOException {
        switch (chunkType) {
            case Boolean:
                throw new UnsupportedOperationException("Booleans are reinterpreted as bytes");
            case Char:
                return CharChunkInputStreamGenerator.extractChunkFromInputStream(
                        Character.BYTES, options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Byte:
                if (type == Boolean.class || type == boolean.class) {
                    return BooleanChunkInputStreamGenerator.extractChunkFromInputStream(
                            options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                return ByteChunkInputStreamGenerator.extractChunkFromInputStream(
                        Byte.BYTES, options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Short:
                return ShortChunkInputStreamGenerator.extractChunkFromInputStream(
                        Short.BYTES, options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Int:
                return IntChunkInputStreamGenerator.extractChunkFromInputStream(
                        Integer.BYTES, options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Long:
                if (factor == 1) {
                    return LongChunkInputStreamGenerator.extractChunkFromInputStream(
                            Long.BYTES, options,
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                }
                return LongChunkInputStreamGenerator.extractChunkFromInputStreamWithConversion(
                        Long.BYTES, options,
                        (long v) -> (v*factor),
                        fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Float:
                return FloatChunkInputStreamGenerator.extractChunkFromInputStream(
                        Float.BYTES, options, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Double:
                return DoubleChunkInputStreamGenerator.extractChunkFromInputStream(
                        Double.BYTES, options,fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
            case Object:
                if (type.isArray()) {
                    if (componentType == byte.class) {
                        return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                                is,
                                fieldNodeIter,
                                bufferInfoIter,
                                (buf, off, len) -> Arrays.copyOfRange(buf, off, off + len),
                                outChunk, outOffset, totalRows
                        );
                    } else {
                        return VarListChunkInputStreamGenerator.extractChunkFromInputStream(
                                options, type, fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                    }
                }
                if (Vector.class.isAssignableFrom(type)) {
                    //noinspection unchecked
                    return VectorChunkInputStreamGenerator.extractChunkFromInputStream(
                            options, (Class<Vector<?>>)type, componentType, fieldNodeIter, bufferInfoIter, is,
                            outChunk, outOffset, totalRows);
                }
                if (type == BigInteger.class) {
                    return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                            is,
                            fieldNodeIter,
                            bufferInfoIter,
                            BigInteger::new,
                            outChunk, outOffset, totalRows
                    );
                }
                if (type == BigDecimal.class) {
                    return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(
                            is,
                            fieldNodeIter,
                            bufferInfoIter,
                            (final byte[] buf, final int offset, final int length) -> {
                                // read the int scale value as little endian, arrow's endianness.
                                final byte b1 = buf[offset];
                                final byte b2 = buf[offset + 1];
                                final byte b3 = buf[offset + 2];
                                final byte b4 = buf[offset + 3];
                                final int scale = b4 << 24 | (b3 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b1 & 0xFF);
                                return new BigDecimal(new BigInteger(buf, offset + 4, length - 4), scale);
                            },
                            outChunk, outOffset, totalRows
                    );
                }
                if (type == Instant.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Long.BYTES, options, io -> DateTimeUtils.epochNanosToInstant(io.readLong()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows
                    );
                }
                if (type == ZonedDateTime.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Long.BYTES, options, io -> DateTimeUtils.epochNanosToZonedDateTime(io.readLong(), DateTimeUtils.timeZone()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows
                    );
                }
                if (type == Byte.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Byte.BYTES, options, io -> TypeUtils.box(io.readByte()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows
                    );
                }
                if (type == Character.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Character.BYTES, options, io -> TypeUtils.box(io.readChar()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows
                    );
                }
                if (type == Double.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Double.BYTES, options, io -> TypeUtils.box(io.readDouble()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows
                    );
                }
                if (type == Float.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Float.BYTES, options, io -> TypeUtils.box(io.readFloat()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows
                    );
                }
                if (type == Integer.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Integer.BYTES, options, io -> TypeUtils.box(io.readInt()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows
                    );
                }
                if (type == Long.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Long.BYTES, options, io -> TypeUtils.box(io.readLong()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows
                    );
                }
                if (type == Short.class) {
                    return FixedWidthChunkInputStreamGenerator.extractChunkFromInputStreamWithTypeConversion(
                            Short.BYTES, options, io -> TypeUtils.box(io.readShort()),
                            fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows
                    );
                }
                if (type == String.class ||
                        options.columnConversionMode().equals(ColumnConversionMode.Stringify)) {
                    return VarBinaryChunkInputStreamGenerator.extractChunkFromInputStream(is, fieldNodeIter, bufferInfoIter,
                            (buf, off, len) -> new String(buf, off, len, Charsets.UTF_8), outChunk, outOffset, totalRows);
                }
                throw new UnsupportedOperationException("Do not yet support column conversion mode: " + options.columnConversionMode());
            default:
                throw new UnsupportedOperationException();
        }
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
        return new ShiftedRangeReader().read(bb);
    }
}
