package io.deephaven.engine.rowset.impl;

import io.deephaven.util.metrics.IntCounterMetric;
import io.deephaven.util.metrics.LongCounterLog2HistogramMetric;
import io.deephaven.util.metrics.LongCounterMetric;
import io.deephaven.util.metrics.MetricsManager;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.IntStartLongDeltaSingleRange;
import io.deephaven.engine.rowset.impl.singlerange.LongStartIntDeltaSingleRange;
import io.deephaven.engine.rowset.impl.singlerange.ShortStartShortDeltaSingleRange;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;

public final class RowSetCounts {
    public final IntCounterMetric emptyCount;

    public final IntCounterMetric shortShortSingleRangeCount;
    public final IntCounterMetric longLongSingleRangeCount;
    public final IntCounterMetric intLongSingleRangeCount;
    public final IntCounterMetric longIntSingleRangeCount;

    public final IntCounterMetric sortedRangesCount;
    public final LongCounterLog2HistogramMetric sortedRangesRowSetCardinality;
    public final LongCounterLog2HistogramMetric sortedRangesRowSetBytesUnused;
    public final LongCounterLog2HistogramMetric sortedRangesRowSetBytesAllocated;

    public final IntCounterMetric rspCount;
    public final LongCounterLog2HistogramMetric rspRowSetCardinality;

    public final LongCounterLog2HistogramMetric rspParallelArraysSizeUsed;
    public final LongCounterLog2HistogramMetric rspParallelArraysSizeUnused;

    public final LongCounterLog2HistogramMetric rspArrayContainersBytesUnused;
    public final LongCounterLog2HistogramMetric rspArrayContainersBytesAllocated;
    public final LongCounterLog2HistogramMetric rspArrayContainersCardinality;
    public final LongCounterMetric rspArrayContainersCount;

    public final LongCounterLog2HistogramMetric rspBitmapContainersBytesUnused;
    public final LongCounterLog2HistogramMetric rspBitmapContainersBytesAllocated;
    public final LongCounterLog2HistogramMetric rspBitmapContainersCardinality;
    public final LongCounterMetric rspBitmapContainersCount;

    public final LongCounterLog2HistogramMetric rspRunContainersBytesUnused;
    public final LongCounterLog2HistogramMetric rspRunContainersBytesAllocated;
    public final LongCounterLog2HistogramMetric rspRunContainersCardinality;
    public final LongCounterMetric rspRunContainersCount;
    public final LongCounterLog2HistogramMetric rspRunContainersRunsCount;
    public final LongCounterMetric rspSingleRangeContainersCount;
    public final LongCounterLog2HistogramMetric rspSingleRangeContainerCardinality;

    public final LongCounterMetric rspTwoValuesContainerCount;
    public final LongCounterMetric rspSingletonContainersCount;

    public final LongCounterLog2HistogramMetric rspFullBlockSpansCount;
    public final LongCounterLog2HistogramMetric rspFullBlockSpansLen;

    public RowSetCounts(final String prefix) {
        emptyCount =
                new IntCounterMetric(prefix + "EmptyCount");

        shortShortSingleRangeCount =
                new IntCounterMetric(prefix + "ShortShortSingleRangeCount");
        longLongSingleRangeCount =
                new IntCounterMetric(prefix + "LongLongSingleRangeCount");
        intLongSingleRangeCount =
                new IntCounterMetric(prefix + "IntLongSingleRangeCount");
        longIntSingleRangeCount =
                new IntCounterMetric(prefix + "LongIntSingleRangeCount");

        sortedRangesCount =
                new IntCounterMetric(prefix + "SortedRangesCount");
        sortedRangesRowSetCardinality =
                new LongCounterLog2HistogramMetric(prefix + "SortedRangesRowSetCardinality");
        sortedRangesRowSetBytesUnused =
                new LongCounterLog2HistogramMetric(prefix + "SortedRangesRowSetBytesUnused");
        sortedRangesRowSetBytesAllocated =
                new LongCounterLog2HistogramMetric(prefix + "SortedRangesRowSetBytesAllocated");

        rspCount =
                new IntCounterMetric(prefix + "RspCount");
        rspRowSetCardinality =
                new LongCounterLog2HistogramMetric(prefix + "RspRowSetCardinality");

        rspParallelArraysSizeUsed =
                new LongCounterLog2HistogramMetric(prefix + "RspParallelArraysSizeUsed");
        rspParallelArraysSizeUnused =
                new LongCounterLog2HistogramMetric(prefix + "RspParallelArraysSizeUnused");

        rspArrayContainersBytesUnused =
                new LongCounterLog2HistogramMetric(prefix + "RspArrayContainersBytesUnused");
        rspArrayContainersBytesAllocated =
                new LongCounterLog2HistogramMetric(prefix + "RspArrayContainersBytesAllocated");
        rspArrayContainersCardinality =
                new LongCounterLog2HistogramMetric(prefix + "RspArrayContainersCardinality");
        rspArrayContainersCount =
                new LongCounterMetric(prefix + "RspArrayContainersCount");

        rspBitmapContainersBytesUnused =
                new LongCounterLog2HistogramMetric(prefix + "RspBitmapContainersBytesUnused");
        rspBitmapContainersBytesAllocated =
                new LongCounterLog2HistogramMetric(prefix + "RspBitmapContainersBytesAllocated");
        rspBitmapContainersCardinality =
                new LongCounterLog2HistogramMetric(prefix + "RspBitmapContainersCardinality");
        rspBitmapContainersCount =
                new LongCounterMetric(prefix + "RspBitmapContainersCount");

        rspRunContainersBytesUnused =
                new LongCounterLog2HistogramMetric(prefix + "RspRunContainersBytesUnused");
        rspRunContainersBytesAllocated =
                new LongCounterLog2HistogramMetric(prefix + "RspRunContainersBytesAllocated");
        rspRunContainersCardinality =
                new LongCounterLog2HistogramMetric(prefix + "RspRunContainersCardinality");
        rspRunContainersCount =
                new LongCounterMetric(prefix + "RspRunContainersCount");
        rspRunContainersRunsCount =
                new LongCounterLog2HistogramMetric(prefix + "RspRunContainersRunCount");
        rspSingleRangeContainersCount =
                new LongCounterMetric(prefix + "RspSingleRangeContainersCount");
        rspSingleRangeContainerCardinality =
                new LongCounterLog2HistogramMetric(prefix + "RspSingleRangeContainerCardinality");

        rspTwoValuesContainerCount =
                new LongCounterMetric(prefix + "RspTwoValuesContainerCount");

        rspSingletonContainersCount =
                new LongCounterMetric(prefix + "RspSingletonContainersCount");

        rspFullBlockSpansCount =
                new LongCounterLog2HistogramMetric(prefix + "RspFullBlockSpansCount");
        rspFullBlockSpansLen =
                new LongCounterLog2HistogramMetric(prefix + "RspFullBlockSpansLen");
    }

    public void sampleRsp(final RspBitmap rb) {
        if (!MetricsManager.enabled) {
            return;
        }
        rspRowSetCardinality.sample(rb.getCardinality());
        rspCount.sample(1);
        rb.sampleMetrics(
                rspParallelArraysSizeUsed,
                rspParallelArraysSizeUnused,
                rspArrayContainersBytesAllocated,
                rspArrayContainersBytesUnused,
                rspArrayContainersCardinality,
                rspArrayContainersCount,
                rspBitmapContainersBytesAllocated,
                rspBitmapContainersBytesUnused,
                rspBitmapContainersCardinality,
                rspBitmapContainersCount,
                rspRunContainersBytesAllocated,
                rspRunContainersBytesUnused,
                rspRunContainersCardinality,
                rspRunContainersCount,
                rspRunContainersRunsCount,
                rspSingleRangeContainersCount,
                rspSingleRangeContainerCardinality,
                rspSingletonContainersCount,
                rspTwoValuesContainerCount,
                rspFullBlockSpansCount,
                rspFullBlockSpansLen);
    }

    public void sampleSingleRange(final SingleRange sr) {
        if (!MetricsManager.enabled) {
            return;
        }
        if (sr instanceof ShortStartShortDeltaSingleRange) {
            shortShortSingleRangeCount.sample(1);
            return;
        }
        if (sr instanceof LongStartIntDeltaSingleRange) {
            longIntSingleRangeCount.sample(1);
            return;
        }
        if (sr instanceof IntStartLongDeltaSingleRange) {
            intLongSingleRangeCount.sample(1);
            return;
        }
        // r instanceof LongStartLongEndSingleRange.
        longLongSingleRangeCount.sample(1);
    }

    public void sampleSortedRanges(final SortedRanges sr) {
        if (!MetricsManager.enabled) {
            return;
        }
        sortedRangesCount.sample(1);
        sortedRangesRowSetCardinality.sample(sr.getCardinality());
        final long allocated = sr.bytesAllocated();
        sortedRangesRowSetBytesAllocated.sample(allocated);
        sortedRangesRowSetBytesUnused.sample(allocated - sr.bytesUsed());
    }

    public void sampleEmpty() {
        if (!MetricsManager.enabled) {
            return;
        }
        emptyCount.sample(1);
    }
}
