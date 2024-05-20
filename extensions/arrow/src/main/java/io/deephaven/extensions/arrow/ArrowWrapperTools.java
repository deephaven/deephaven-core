//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.arrow;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.reference.WeakCleanupReference;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ResettableContext;
import io.deephaven.engine.util.file.FileHandle;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.extensions.arrow.sources.ArrowByteColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowCharColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowInstantColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowIntColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowLocalTimeColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowLongColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowObjectColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowStringColumnSource;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.extensions.arrow.sources.ArrowBooleanColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowDoubleColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowFloatColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowShortColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowUInt1ColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowUInt4ColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowUInt8ColumnSource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.WeakReference;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.time.LocalDateTime;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.datastructures.SizeException;
import io.deephaven.util.reference.CleanupReferenceProcessor;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;
import org.jetbrains.annotations.NotNull;

import static org.apache.arrow.vector.ipc.message.MessageSerializer.IPC_CONTINUATION_TOKEN;

/**
 * This class contains tools for dealing apache arrow data.
 * <ul>
 * <li>{@link ArrowBooleanColumnSource} - uses {@link BitVector} under the hood, returns Boolean</li>
 * <li>{@link ArrowByteColumnSource} - uses {@link TinyIntVector} under the hood, returns byte</li>
 * <li>{@link ArrowCharColumnSource} - uses {@link UInt2Vector} under the hood, returns long</li>
 * <li>{@link ArrowFloatColumnSource} - uses {@link Float4Vector} under the hood, returns float</li>
 * <li>{@link ArrowDoubleColumnSource} - uses {@link Float8Vector} under the hood, returns double</li>
 * <li>{@link ArrowShortColumnSource} - uses {@link SmallIntVector} under the hood, returns short</li>
 * <li>{@link ArrowIntColumnSource} - uses {@link IntVector} under the hood, returns int</li>
 * <li>{@link ArrowLongColumnSource} - uses {@link BigIntVector} under the hood, returns long</li>
 * <li>{@link ArrowUInt1ColumnSource} - uses {@link UInt1Vector} under the hood, returns short</li>
 * <li>{@link ArrowUInt4ColumnSource} - uses {@link UInt4Vector} under the hood, returns long</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;BigInteger&gt;} - uses {@link UInt8Vector} under the
 * hood, returns BigInteger</li>
 * <li>{@link ArrowLocalTimeColumnSource} - uses {@link TimeMilliVector} under the hood, returns LocalTime</li>
 * <li>{@link ArrowInstantColumnSource} - uses {@link TimeStampVector} under the hood, returns DateTime</li>
 * <li>{@link ArrowStringColumnSource} - uses {@link VarCharVector} under the hood, returns String</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;byte[]&gt;} - uses {@link FixedSizeBinaryVector} under
 * the hood, returns byte[]</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;byte[]&gt;} - uses {@link VarBinaryVector} under the
 * hood, returns byte[]</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;BigDecimal&gt;} - uses {@link DecimalVector} under the
 * hood, returns BigDecimal</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;BigDecimal&gt;} - uses {@link Decimal256Vector} under
 * the hood, returns BigDecimal</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;LocalDateTime&gt;} - uses {@link DateMilliVector} under
 * the hood, returns LocalDateTime</li>
 * </ul>
 *
 * <p>
 * Note that Arrow's Java implementation only supports feather v2, as a result this class only supports feather v2.
 * <p>
 * There are some performance issues under certain data access patterns. See deephaven-core#3633 for details and
 * suggested future improvements.
 */
public class ArrowWrapperTools {
    private static final int MAX_POOL_SIZE = Configuration.getInstance().getIntegerWithDefault(
            "ArrowWrapperTools.defaultMaxPooledContext", Runtime.getRuntime().availableProcessors());

    private static final BufferAllocator rootAllocator = new RootAllocator();

    /**
     * Reads arrow data from a feather-formatted file and returns a query table
     */
    public static QueryTable readFeather(@NotNull final String path) {
        final ArrowTableContext arrowTableContext = new ArrowTableContext(path, rootAllocator);
        try (final Shareable context = arrowTableContext.newShareable();
                final SeekableByteChannel channel = arrowTableContext.openChannel()) {
            final ArrowFileReader reader = context.getReader();
            int biggestBlock = 0;
            final List<ArrowBlock> recordBlocks = reader.getRecordBlocks();
            final int[] blocks = new int[recordBlocks.size()];

            int metadataBufLen = 1024;
            byte[] rawMetadataBuf = new byte[metadataBufLen];
            final Message message = new Message();
            final RecordBatch recordBatch = new RecordBatch();
            for (int ii = 0; ii < blocks.length; ++ii) {
                final ArrowBlock block = recordBlocks.get(ii);
                if (block.getMetadataLength() > ArrayUtil.MAX_ARRAY_SIZE) {
                    throw new SizeException("Metadata length exceeds maximum array size. Failed reading block " + ii
                            + " of '" + path + "'", block.getMetadataLength(), ArrayUtil.MAX_ARRAY_SIZE);
                }
                while (block.getMetadataLength() > metadataBufLen) {
                    metadataBufLen = Math.min(2 * metadataBufLen, ArrayUtil.MAX_ARRAY_SIZE);
                    rawMetadataBuf = new byte[metadataBufLen];
                }

                channel.position(block.getOffset());

                final ByteBuffer metadataBuf = ByteBuffer.wrap(rawMetadataBuf, 0, block.getMetadataLength());
                while (metadataBuf.hasRemaining()) {
                    final int read = channel.read(metadataBuf);
                    if (read == 0) {
                        throw new IllegalStateException("ReadableByteChannel.read returned 0");
                    }
                    if (read == -1) {
                        throw new IOException(
                                "Unexpected end of input trying to read block " + ii + " of '" + path + "'");
                    }
                }
                metadataBuf.flip();
                if (metadataBuf.getInt() == IPC_CONTINUATION_TOKEN) {
                    // if the continuation token is present, skip the length
                    metadataBuf.position(metadataBuf.position() + Integer.BYTES);
                }
                Message.getRootAsMessage(metadataBuf.asReadOnlyBuffer(), message);
                message.header(recordBatch);

                final int rowCount = LongSizedDataStructure.intSize("ArrowWrapperTools#readFeather",
                        recordBatch.length());
                blocks[ii] = rowCount;
                biggestBlock = Math.max(biggestBlock, rowCount);
            }

            // note we can use `0` to index the first row of each block; e.g. 16 rows needs only 4 bits
            final int highBit = Integer.highestOneBit(biggestBlock - 1) << 1;
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            for (int bi = 0; bi < blocks.length; bi++) {
                final long rangeStart = (long) highBit * bi;
                builder.appendRange(rangeStart, rangeStart + blocks[bi] - 1);
            }

            final VectorSchemaRoot root = context.getReader().getVectorSchemaRoot();
            final Map<String, AbstractColumnSource<?>> sources = root.getSchema()
                    .getFields().stream().collect(Collectors.toMap(
                            Field::getName,
                            f -> generateColumnSource(root.getVector(f), f, highBit, arrowTableContext),
                            (u, v) -> {
                                throw new IllegalStateException(String.format("Duplicate key %s", u));
                            },
                            LinkedHashMap::new));

            return new QueryTable(builder.build().toTracking(), sources) {
                @Override
                public void releaseCachedResources() {
                    arrowTableContext.releaseCachedResources();
                }
            };
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static AbstractColumnSource<?> generateColumnSource(
            final FieldVector vector, final Field field, final int highBit, final ArrowTableContext arrowHelper) {
        switch (vector.getMinorType()) {
            case TINYINT:
                return new ArrowByteColumnSource(highBit, field, arrowHelper);
            case SMALLINT:
                return new ArrowShortColumnSource(highBit, field, arrowHelper);
            case INT:
                return new ArrowIntColumnSource(highBit, field, arrowHelper);
            case BIGINT:
                return new ArrowLongColumnSource(highBit, field, arrowHelper);
            case DATEMILLI:
                return new ArrowObjectColumnSource<>(LocalDateTime.class, highBit, field, arrowHelper);
            case TIMEMILLI:
                return new ArrowLocalTimeColumnSource(highBit, field, arrowHelper);
            case FLOAT4:
                return new ArrowFloatColumnSource(highBit, field, arrowHelper);
            case FLOAT8:
                return new ArrowDoubleColumnSource(highBit, field, arrowHelper);
            case BIT:
                return new ArrowBooleanColumnSource(highBit, field, arrowHelper);
            case VARCHAR:
                return new ArrowStringColumnSource(highBit, field, arrowHelper);
            case VARBINARY:
            case FIXEDSIZEBINARY:
                return new ArrowObjectColumnSource<>(byte[].class, highBit, field, arrowHelper);
            case DECIMAL:
            case DECIMAL256:
                return new ArrowObjectColumnSource<>(BigDecimal.class, highBit, field, arrowHelper);
            case UINT1:
                return new ArrowUInt1ColumnSource(highBit, field, arrowHelper);
            case UINT2:
                return new ArrowCharColumnSource(highBit, field, arrowHelper);
            case UINT4:
                return new ArrowUInt4ColumnSource(highBit, field, arrowHelper);
            case UINT8:
                return new ArrowUInt8ColumnSource(highBit, field, arrowHelper);
            case TIMESTAMPMICRO:
            case TIMESTAMPMICROTZ:
            case TIMESTAMPMILLI:
            case TIMESTAMPMILLITZ:
            case TIMESTAMPNANO:
            case TIMESTAMPNANOTZ:
            case TIMESTAMPSEC:
            case TIMESTAMPSECTZ:
                return new ArrowInstantColumnSource(highBit, field, arrowHelper);
            case NULL:
            case STRUCT:
            case DATEDAY:
            case TIMESEC:
            case TIMEMICRO:
            case TIMENANO:
            case INTERVALDAY:
            case INTERVALMONTHDAYNANO:
            case DURATION:
            case INTERVALYEAR:
            case LARGEVARCHAR:
            case LARGEVARBINARY:
            case LIST:
            case LARGELIST:
            case FIXED_SIZE_LIST:
            case UNION:
            case DENSEUNION:
            case MAP:
            case EXTENSIONTYPE:
                break;
        }
        throw new TableDataException(
                String.format("Unsupported vector: name: %s type: %s", vector.getName(), vector.getMinorType()));
    }

    private static final class SharingKey extends SharedContext.ExactReferenceSharingKey<Shareable> {
        public SharingKey(@NotNull final ArrowWrapperTools.ArrowTableContext arrowHelper) {
            super(arrowHelper);
        }
    }

    public static class FillContext implements ColumnSource.FillContext {
        private final boolean shared;
        private final Shareable shareable;

        public FillContext(final ArrowTableContext helper, final SharedContext sharedContext) {
            this.shared = sharedContext != null;
            shareable = sharedContext == null ? helper.newShareable()
                    : sharedContext.getOrCreate(new SharingKey(helper), helper::newShareable);
        }

        public <T extends FieldVector> T getVector(final Field field) {
            try {
                // noinspection unchecked
                return (T) shareable.getReader().getVectorSchemaRoot().getVector(field);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void ensureLoadingBlock(final int blockNumber) {
            this.shareable.ensureLoadingBlock(blockNumber);
        }

        @Override
        public void close() {
            if (!shared) {
                shareable.close();
            }
        }
    }

    public static final class Shareable implements ResettableContext {
        private static final AtomicInteger numBlocksLoaded = new AtomicInteger();

        private final WeakReference<ArrowTableContext> tableContext;
        private final ArrowFileReader reader;
        @ReferentialIntegrity
        private final ReaderCleanup cleanup;

        private int blockNo = QueryConstants.NULL_INT;


        Shareable(ArrowTableContext tableContext, ArrowFileReader reader) {
            this.tableContext = new WeakReference<>(tableContext);
            this.reader = reader;
            this.cleanup = new ReaderCleanup(this);
        }

        public ArrowFileReader getReader() {
            return this.reader;
        }

        @TestUseOnly
        public static int numBlocksLoaded() {
            return numBlocksLoaded.get();
        }

        @TestUseOnly
        public static void resetNumBlocksLoaded() {
            numBlocksLoaded.set(0);
        }

        @Override
        public void reset() {
            // Nothing to do here.
        }

        @Override
        public void close() {
            ArrowTableContext tableContext = this.tableContext.get();
            if (tableContext != null) {
                tableContext.addToPool(this);
            } else {
                destroy();
            }
        }

        public void destroy() {
            try {
                this.reader.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        void ensureLoadingBlock(final int blockNumber) {
            if (blockNumber != blockNo) {
                try {
                    reader.loadRecordBatch(reader.getRecordBlocks().get(blockNumber));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                numBlocksLoaded.incrementAndGet();
                blockNo = blockNumber;
            }
        }
    }

    public static class ArrowTableContext {
        private final String path;

        private final BufferAllocator rootAllocator;

        private final Deque<Shareable> readerStack = new ConcurrentLinkedDeque<>();

        public ArrowTableContext(final String path, final BufferAllocator rootAllocator) {
            this.path = path;
            this.rootAllocator = rootAllocator;
        }

        private Shareable newShareable() {
            Shareable reader = readerStack.poll();
            if (reader != null) {
                return reader;
            }
            final SeekableByteChannel channel;
            try {
                channel = openChannel();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return new Shareable(this, new ArrowFileReader(
                    channel, rootAllocator, CommonsCompressionFactory.INSTANCE));
        }

        @NotNull
        private FileHandle openChannel() throws IOException {
            return TrackedFileHandleFactory.getInstance().readOnlyHandleCreator.invoke(new File(path));
        }

        private void addToPool(final Shareable context) {
            // this comparison is not atomic, but it is ok to be close enough
            if (readerStack.size() >= MAX_POOL_SIZE) {
                context.destroy();
            } else {
                readerStack.push(context);
            }
        }

        public void releaseCachedResources() {
            Shareable toDestroy;
            while ((toDestroy = readerStack.poll()) != null) {
                toDestroy.destroy();
            }
        }
    }

    private static final class ReaderCleanup extends WeakCleanupReference<Shareable> {
        private final ArrowFileReader reader;

        private ReaderCleanup(final Shareable shareable) {
            super(shareable, CleanupReferenceProcessor.getDefault().getReferenceQueue());
            this.reader = shareable.getReader();
        }

        @Override
        public void cleanup() {
            try {
                reader.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
