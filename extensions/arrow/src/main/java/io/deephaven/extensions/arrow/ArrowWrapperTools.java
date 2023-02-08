package io.deephaven.extensions.arrow;

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
import io.deephaven.engine.rowset.TrackingRowSet;
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
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

/**
 * <pre>
 * This class contains tools for dealing apache arrow data.
 * {@link ArrowBooleanColumnSource} - uses {@link BitVector} under the hood, returns Boolean
 * {@link ArrowByteColumnSource} - uses {@link TinyIntVector} under the hood, returns byte
 * {@link ArrowCharColumnSource} - uses {@link UInt2Vector} under the hood, returns long
 * {@link ArrowFloatColumnSource} - uses {@link Float4Vector} under the hood, returns float
 * {@link ArrowDoubleColumnSource} - uses {@link Float8Vector} under the hood, returns double
 * {@link ArrowShortColumnSource} - uses {@link SmallIntVector} under the hood, returns short
 * {@link ArrowIntColumnSource} - uses {@link IntVector} under the hood, returns int
 * {@link ArrowLongColumnSource} - uses {@link BigIntVector} under the hood, returns long
 * {@link ArrowUInt1ColumnSource} - uses {@link UInt1Vector} under the hood, returns short
 * {@link ArrowUInt4ColumnSource} - uses {@link UInt4Vector} under the hood, returns long
 * {@link ArrowObjectColumnSource<BigInteger>} - uses {@link UInt8Vector} under the hood, returns BigInteger
 * {@link ArrowLocalTimeColumnSource} - uses {@link TimeMilliVector} under the hood, returns LocalTime
 * {@link ArrowDateTimeColumnSource} - uses {@link TimeStampVector} under the hood, returns DateTime
 * {@link ArrowStringColumnSource} - uses {@link VarCharVector} under the hood, returns String
 * {@link ArrowObjectColumnSource<byte[]>} - uses {@link FixedSizeBinaryVector} under the hood, returns byte[]
 * {@link ArrowObjectColumnSource<byte[]>} - uses {@link VarBinaryVector} under the hood, returns byte[]
 * {@link ArrowObjectColumnSource<BigDecimal>} - uses {@link DecimalVector} under the hood, returns BigDecimal
 * {@link ArrowObjectColumnSource<BigDecimal>} - uses {@link Decimal256Vector} under the hood, returns BigDecimal
 * {@link ArrowObjectColumnSource<LocalDateTime>} - uses {@link DateMilliVector} under the hood, returns LocalDateTime
 * </pre>
 */
public class ArrowWrapperTools {
    private static final BufferAllocator rootAllocator = new RootAllocator();

    /**
     * Reads arrow data from file and returns a query table
     */
    public static QueryTable readArrow(final @NotNull String path) {
        final Helper helper = new Helper(path, rootAllocator);
        final Shareable context = helper.newShareable();
        final ArrowFileReader reader = context.getReader();
        try {
            int biggestBlock = 0;
            final List<ArrowBlock> recordBlocks = reader.getRecordBlocks();
            final int[] blocks = new int[recordBlocks.size()];
            for (int i = 0; i < recordBlocks.size(); i++) {
                reader.loadRecordBatch(recordBlocks.get(i));
                final int schemaRowCount = reader.getVectorSchemaRoot().getRowCount();
                blocks[i] = schemaRowCount;
                if (schemaRowCount > biggestBlock) {
                    biggestBlock = schemaRowCount;
                }
            }
            final int highBit = Integer.highestOneBit(biggestBlock) << 1;
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            for (int i = 0; i < blocks.length; i++) {
                final long rangeStart = (long) highBit * i;
                builder.appendRange(rangeStart, rangeStart + blocks[i] - 1);
            }
            return getQueryTable(context, highBit, builder.build().toTracking(), helper);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            helper.addToPool(context);
        }
    }

    private static QueryTable getQueryTable(
            final Shareable context,
            final int highBit,
            final TrackingRowSet rowSet,
            final Helper helper) throws IOException {
        VectorSchemaRoot vectorSchemaRoot = context.getReader().getVectorSchemaRoot();
        Map<String, AbstractColumnSource<?>> sources = vectorSchemaRoot.getSchema().getFields().stream()
                .collect(Collectors.toMap(Field::getName,
                        f -> generateColumnSource(context, f, highBit, helper)));
        return new ArrowWrappedTable(rowSet, sources, helper);
    }

    private static AbstractColumnSource<?> generateColumnSource(final Shareable context,
            final Field field, final int highBit, final Helper arrowHelper) {
        FieldVector vector;
        try {
            vector = context.getReader().getVectorSchemaRoot().getVector(field);
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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            arrowHelper.addToPool(context);
        }
    }

    private static class ArrowWrappedTable extends QueryTable {
        private final ArrowWrapperTools.Helper helper;

        public ArrowWrappedTable(
                final TrackingRowSet rowSet,
                final Map<String, ? extends ColumnSource<?>> columns,
                final ArrowWrapperTools.Helper helper) {
            super(rowSet, columns);
            this.helper = helper;
        }

        @Override
        public void close() {
            super.close();
            this.helper.close();
        }
    }

    private static final class SharingKey extends SharedContext.ExactReferenceSharingKey<Shareable> {
        public SharingKey(@NotNull final Helper arrowHelper) {
            super(arrowHelper);
        }
    }

    public static class FillContext implements ColumnSource.FillContext {
        private final boolean shared;
        private final Shareable shareable;
        private final Helper helper;

        public FillContext(final Helper helper, final SharedContext sharedContext) {
            this.helper = helper;
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
                helper.addToPool(shareable);
            }
        }
    }

    public static final class Shareable extends SharedContext {
        private final ArrowFileReader reader;
        private int blockNo = QueryConstants.NULL_INT;

        private static final AtomicInteger numBlocksLoaded = new AtomicInteger();

        Shareable(ArrowFileReader reader) {
            this.reader = reader;
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
        public void close() {
            super.close();
            try {
                this.reader.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        void ensureLoadingBlock(final int blockNumber) {
            if (blockNumber != blockNo) {
                ArrowFileReader reader = this.getReader();
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

    public static class Helper implements SafeCloseable {
        private final String path;

        private final BufferAllocator rootAllocator;

        private final Deque<Shareable> readerStack = new ConcurrentLinkedDeque<>();

        public Helper(final String path, final BufferAllocator rootAllocator) {
            this.path = path;
            this.rootAllocator = rootAllocator;
        }

        public Shareable newShareable() {
            Shareable reader = readerStack.poll();
            if (reader != null) {
                return reader;
            }
            File file = new File(path);
            FileInputStream fileInputStream;
            try {
                // this input stream is closed when the ArrowFileReader is closed
                fileInputStream = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                throw new UncheckedIOException(e);
            }
            return new Shareable(new ArrowFileReader(fileInputStream.getChannel(), rootAllocator));
        }

        private void addToPool(final Shareable context) {
            this.readerStack.push(context);
        }

        public void close() {
            readerStack.forEach(Shareable::close);
            readerStack.clear();
        }
    }
}
