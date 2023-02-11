package io.deephaven.extensions.arrow;

import io.deephaven.base.reference.WeakCleanupReference;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ResettableContext;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.file.FileHandle;
import io.deephaven.engine.util.file.TrackedFileHandleFactory;
import io.deephaven.engine.util.reference.CleanupReferenceProcessorInstance;
import io.deephaven.extensions.arrow.sources.ArrowByteColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowCharColumnSource;
import io.deephaven.extensions.arrow.sources.ArrowDateTimeColumnSource;
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
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
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
 * <pre>
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
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;BigInteger&gt;} - uses {@link UInt8Vector} under the hood, returns BigInteger</li>
 * <li>{@link ArrowLocalTimeColumnSource} - uses {@link TimeMilliVector} under the hood, returns LocalTime</li>
 * <li>{@link ArrowDateTimeColumnSource} - uses {@link TimeStampVector} under the hood, returns DateTime</li>
 * <li>{@link ArrowStringColumnSource} - uses {@link VarCharVector} under the hood, returns String</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;byte[]&gt;} - uses {@link FixedSizeBinaryVector} under the hood, returns byte[]</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;byte[]&gt;} - uses {@link VarBinaryVector} under the hood, returns byte[]</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;BigDecimal&gt;} - uses {@link DecimalVector} under the hood, returns BigDecimal</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;BigDecimal&gt;} - uses {@link Decimal256Vector} under the hood, returns BigDecimal</li>
 * <li>{@link ArrowObjectColumnSource ArrowObjectColumnSource&lt;LocalDateTime&gt;} - uses {@link DateMilliVector} under the hood, returns LocalDateTime</li>
 * </ul>
 * </pre>
 */
public class ArrowWrapperTools {
    private static final int MAX_POOL_SIZE = Math.max(UpdateGraphProcessor.DEFAULT.getUpdateThreads(),
            Configuration.getInstance().getIntegerWithDefault("ArrowWrapperTools.defaultMaxPooledContext", 4));

    private static final BufferAllocator rootAllocator = new RootAllocator();

    /**
     * Reads arrow data from a feather-formatted file and returns a query table
     */
    public static QueryTable readFeather(final @NotNull String path) {
        final ArrowTableContext arrowTableContext = new ArrowTableContext(path, rootAllocator);
        try (final Shareable context = arrowTableContext.newShareable();
                final SeekableByteChannel channel = arrowTableContext.openChannel()) {
            final ArrowFileReader reader = context.getReader();
            int biggestBlock = 0;
            final List<ArrowBlock> recordBlocks = reader.getRecordBlocks();
            final int[] blocks = new int[recordBlocks.size()];
            // TODO: load only the metadata to speed up initial read time

            int metadataBufLen = 1024;
            byte[] rawMetadataBuf = new byte[metadataBufLen];
            for (int ii = 0; ii < blocks.length; ++ii) {
                final ArrowBlock block = recordBlocks.get(ii);
                while (block.getMetadataLength() > metadataBufLen) {
                    metadataBufLen *= 2;
                    rawMetadataBuf = new byte[metadataBufLen];
                }

                channel.position(block.getOffset());

                final ByteBuffer metadataBuf = ByteBuffer.wrap(rawMetadataBuf, 0, block.getMetadataLength());
                int numRead = channel.read(metadataBuf);
                if (numRead != block.getMetadataLength()) {
                    throw new IOException("Unexpected end of input trying to read batch.");
                }

                metadataBuf.flip();
                if (metadataBuf.getInt() == IPC_CONTINUATION_TOKEN) {
                    // if the continuation token is present, skip the length
                    metadataBuf.getInt();
                }
                final Message message = Message.getRootAsMessage(metadataBuf.asReadOnlyBuffer());
                final RecordBatch batch = (RecordBatch) message.header(new RecordBatch());

                final int rowCount = LongSizedDataStructure.intSize("ArrowWrapperTools#readFeather", batch.length());
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
        // TODO: use switch expression on MinorType
        if (vector instanceof BitVector) {
            return new ArrowBooleanColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof TinyIntVector) {
            return new ArrowByteColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof SmallIntVector) {
            return new ArrowShortColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof IntVector) {
            return new ArrowIntColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof BigIntVector) {
            return new ArrowLongColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof Float4Vector) {
            return new ArrowFloatColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof Float8Vector) {
            return new ArrowDoubleColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof VarCharVector) {
            return new ArrowStringColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof UInt1Vector) {
            return new ArrowUInt1ColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof UInt2Vector) {
            return new ArrowCharColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof UInt4Vector) {
            return new ArrowUInt4ColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof UInt8Vector) {
            return new ArrowUInt8ColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof FixedSizeBinaryVector) {
            return new ArrowObjectColumnSource<>(byte[].class, highBit, field, arrowHelper);
        } else if (vector instanceof VarBinaryVector) {
            return new ArrowObjectColumnSource<>(byte[].class, highBit, field, arrowHelper);
        } else if (vector instanceof DecimalVector) {
            return new ArrowObjectColumnSource<>(BigDecimal.class, highBit, field, arrowHelper);
        } else if (vector instanceof Decimal256Vector) {
            return new ArrowObjectColumnSource<>(BigDecimal.class, highBit, field, arrowHelper);
        } else if (vector instanceof DateMilliVector) {
            return new ArrowObjectColumnSource<>(LocalDateTime.class, highBit, field, arrowHelper);
        } else if (vector instanceof TimeMilliVector) {
            return new ArrowLocalTimeColumnSource(highBit, field, arrowHelper);
        } else if (vector instanceof TimeStampVector) {
            return new ArrowDateTimeColumnSource(highBit, field, arrowHelper);
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
            return new Shareable(this, new ArrowFileReader(channel, rootAllocator));
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
            super(shareable, CleanupReferenceProcessorInstance.DEFAULT.getReferenceQueue());
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
