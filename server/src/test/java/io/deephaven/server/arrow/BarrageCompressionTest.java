//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageCompressionAttribute;
import io.deephaven.server.hierarchicaltable.HierarchicalTableView;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.deephaven.engine.table.Table.BARRAGE_COMPRESSION_ATTRIBUTE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Covers how {@link BarrageCompression} picks a response compression type, including for rollup and tree views, whose
 * attributes come from the hierarchical table rather than from its source.
 */
public class BarrageCompressionTest {

    private SafeCloseable executionContext;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    @After
    public void tearDown() {
        executionContext.close();
    }

    @Test
    public void chooseFirstAllowedTypeTheClientAccepts() {
        final Table table = TableTools.emptyTable(10)
                .withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd,snappy,gzip"));

        assertEquals(Optional.of("snappy"), apply(Set.of("gzip", "snappy"), table));
        assertEquals(Optional.of("zstd"), apply(Set.of("identity", "gzip", "zstd"), table));
        assertEquals(Optional.empty(), apply(Set.of("identity", "br"), table));
        assertEquals(Optional.empty(), apply(Set.of(), table));
    }

    @Test
    public void typesFromWithCompressionAreChosen() {
        final Table table = BarrageCompressionAttribute.withCompression(TableTools.emptyTable(10), "snappy", "gzip");
        assertEquals(Optional.of("snappy"), apply(Set.of("gzip", "zstd", "snappy"), table));
        assertEquals(Optional.of("gzip"), apply(Set.of("gzip", "br"), table));
    }

    @Test
    public void tablesWithoutAValidListAreNotCompressed() {
        assertEquals(Optional.empty(), apply(Set.of("gzip", "zstd"), TableTools.emptyTable(10)));
        final Table malformed =
                TableTools.emptyTable(10).withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd,not-a-codec"));
        assertEquals(Optional.empty(), apply(Set.of("gzip", "zstd"), malformed));
        // parsed even for a client that accepts nothing; not logged again, since the first fetch logged this value
        assertEquals(Optional.empty(), apply(Set.of(), malformed));
        assertEquals(Optional.empty(), apply(Set.of("gzip", "zstd"), null));
    }

    @Test
    public void serverDefaultAppliesWithoutAValidList() {
        final List<String> defaults = List.of("snappy", "gzip");

        // no attribute: the server's default
        assertEquals(Optional.of("snappy"), apply(Set.of("gzip", "snappy"), TableTools.emptyTable(10), defaults));
        assertEquals(Optional.of("gzip"), apply(Set.of("gzip"), TableTools.emptyTable(10), defaults));
        // a malformed attribute is ignored, as if unset
        final Table malformed =
                TableTools.emptyTable(10).withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd,not-a-codec"));
        assertEquals(Optional.of("snappy"), apply(Set.of("zstd", "snappy"), malformed, defaults));
        // the table's own list replaces the default
        final Table zstd = BarrageCompressionAttribute.withCompression(TableTools.emptyTable(10), "zstd");
        assertEquals(Optional.of("zstd"), apply(Set.of("zstd", "snappy"), zstd, defaults));
        assertEquals(Optional.empty(), apply(Set.of("gzip", "snappy"), zstd, defaults));
        // an empty list opts out of the default
        final Table never = BarrageCompressionAttribute.withCompression(TableTools.emptyTable(10));
        assertEquals(Optional.empty(), apply(Set.of("gzip", "snappy"), never, defaults));
        // an object without attributes is never compressed
        assertEquals(Optional.empty(), apply(Set.of("gzip", "snappy"), null, defaults));
    }

    @Test
    public void onlyServerCallObserversAreCompressed() {
        final Table table = TableTools.emptyTable(10).withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd"));
        final StreamObserver<InputStream> plain = new StreamObserver<>() {
            @Override
            public void onNext(final InputStream value) {}

            @Override
            public void onError(final Throwable t) {}

            @Override
            public void onCompleted() {}
        };
        assertEquals(Optional.empty(), BarrageCompression.apply(plain, Set.of("zstd"), table, "plain"));
    }

    @Test
    public void codecsMissingFromTheServerRegistryAreNotUsed() {
        final Table table = TableTools.emptyTable(10).withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd"));
        // ServerCallImpl.setCompression throws this when the server's compressor registry has no such codec
        final RecordingObserver noCodecs = new RecordingObserver() {
            @Override
            public void setCompression(final String compression) {
                throw new IllegalArgumentException("Unable to find compressor by name " + compression);
            }
        };
        assertEquals(Optional.empty(), BarrageCompression.apply(noCodecs, Set.of("zstd"), table, "noCodecs"));
    }

    @Test
    public void codecsMissingFromTheServerRegistryFallThroughToTheNextType() {
        final Table table =
                TableTools.emptyTable(10).withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd,snappy,gzip"));
        // a server with only gzip, as the Netty server is
        final RecordingObserver gzipOnly = new RecordingObserver() {
            @Override
            public void setCompression(final String compression) {
                if (!compression.equals("gzip")) {
                    throw new IllegalArgumentException("Unable to find compressor by name " + compression);
                }
                super.setCompression(compression);
            }
        };
        assertEquals(Optional.of("gzip"),
                BarrageCompression.apply(gzipOnly, Set.of("gzip", "zstd", "snappy"), table, "gzipOnly"));
        assertEquals("gzip", gzipOnly.compression);
        // nothing left to fall through to
        assertEquals(Optional.empty(),
                BarrageCompression.apply(gzipOnly, Set.of("zstd", "snappy"), table, "gzipOnly"));
    }

    @Test
    public void rollupViewsUseTheRollupsAttributes() {
        final Table source = TableTools.emptyTable(10).update("A = ii % 3", "B = ii");
        final HierarchicalTableViewExchangeMarshaller marshaller =
                new HierarchicalTableViewExchangeMarshaller((view, listener, options, intervalMillis) -> {
                    throw new UnsupportedOperationException("not subscribed in this test");
                });

        // set on the rollup: used
        final RollupTable rollup = source.rollup(List.of(Aggregation.AggSum("B")), "A")
                .withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd"));
        final HierarchicalTableView rollupView = HierarchicalTableView.makeFromHierarchicalTable(rollup);
        assertEquals("zstd", marshaller.attributesFor(rollupView).getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertEquals(Optional.of("zstd"), apply(Set.of("gzip", "zstd"), marshaller.attributesFor(rollupView)));

        // set only on the source: not carried to the rollup, so ignored
        final RollupTable fromCompressedSource = source
                .withAttributes(Map.of(BARRAGE_COMPRESSION_ATTRIBUTE, "zstd"))
                .rollup(List.of(Aggregation.AggSum("B")), "A");
        final HierarchicalTableView sourceOnlyView =
                HierarchicalTableView.makeFromHierarchicalTable(fromCompressedSource);
        assertNull(marshaller.attributesFor(sourceOnlyView).getAttribute(BARRAGE_COMPRESSION_ATTRIBUTE));
        assertEquals(Optional.empty(), apply(Set.of("gzip", "zstd"), marshaller.attributesFor(sourceOnlyView)));
        // so the server's default applies to it
        assertEquals(Optional.of("gzip"),
                apply(Set.of("gzip", "zstd"), marshaller.attributesFor(sourceOnlyView), List.of("gzip")));
    }

    /**
     * Applies compression to a fresh recording observer and checks that the observer saw the returned compression type.
     */
    private static Optional<String> apply(
            final Set<String> accepted,
            final io.deephaven.engine.table.AttributeMap<?> attributes) {
        final RecordingObserver observer = new RecordingObserver();
        final Optional<String> chosen = BarrageCompression.apply(observer, accepted, attributes, "test");
        assertEquals(chosen.orElse(null), observer.compression);
        return chosen;
    }

    /**
     * Like {@link #apply(Set, io.deephaven.engine.table.AttributeMap)}, with {@code defaultAllowed} as the server's
     * default compression types.
     */
    private static Optional<String> apply(
            final Set<String> accepted,
            final io.deephaven.engine.table.AttributeMap<?> attributes,
            final List<String> defaultAllowed) {
        final RecordingObserver observer = new RecordingObserver();
        final Optional<String> chosen =
                BarrageCompression.apply(observer, accepted, attributes, "test", defaultAllowed);
        assertEquals(chosen.orElse(null), observer.compression);
        return chosen;
    }

    private static class RecordingObserver extends ServerCallStreamObserver<InputStream> {
        private String compression;

        @Override
        public void setCompression(final String compression) {
            this.compression = compression;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(final Runnable onCancelHandler) {}

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setOnReadyHandler(final Runnable onReadyHandler) {}

        @Override
        public void request(final int count) {}

        @Override
        public void setMessageCompression(final boolean enable) {}

        @Override
        public void disableAutoInboundFlowControl() {}

        @Override
        public void onNext(final InputStream value) {}

        @Override
        public void onError(final Throwable t) {}

        @Override
        public void onCompleted() {}
    }
}
