package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.utils.metrics.IntCounterMetric;
import io.deephaven.db.v2.utils.metrics.LongCounterLog2HistogramMetric;
import io.deephaven.db.v2.utils.metrics.LongCounterMetric;
import io.deephaven.db.v2.utils.metrics.MetricsManager;
import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.db.v2.utils.singlerange.IntStartLongDeltaSingleRange;
import io.deephaven.db.v2.utils.singlerange.LongStartIntDeltaSingleRange;
import io.deephaven.db.v2.utils.singlerange.ShortStartShortDeltaSingleRange;
import io.deephaven.db.v2.utils.singlerange.SingleRange;
import io.deephaven.db.v2.utils.sortedranges.SortedRanges;

public final class IndexCounts {
    public final IntCounterMetric emptyCount;

    public final IntCounterMetric shortShortSingleRangeCount;
    public final IntCounterMetric longLongSingleRangeCount;
    public final IntCounterMetric intLongSingleRangeCount;
    public final IntCounterMetric longIntSingleRangeCount;

    public final IntCounterMetric sortedRangesCount;
    public final LongCounterLog2HistogramMetric sortedRangesIndexCardinality;
    public final LongCounterLog2HistogramMetric sortedRangesIndexBytesUnused;
    public final LongCounterLog2HistogramMetric sortedRangesIndexBytesAllocated;

    public final IntCounterMetric rspCount;
    public final LongCounterLog2HistogramMetric rspIndexCardinality;

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

    public IndexCounts(final String prefix) {
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
        sortedRangesIndexCardinality =
                new LongCounterLog2HistogramMetric(prefix + "SortedRangesIndexCardinality");
        sortedRangesIndexBytesUnused =
                new LongCounterLog2HistogramMetric(prefix + "SortedRangesIndexBytesUnused");
        sortedRangesIndexBytesAllocated =
                new LongCounterLog2HistogramMetric(prefix + "SortedRangesIndexBytesAllocated");

        rspCount =
                new IntCounterMetric(prefix + "RspCount");
        rspIndexCardinality =
                new LongCounterLog2HistogramMetric(prefix + "RspIndexCardinality");

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
    }

    public void sampleRsp(final RspBitmap rb) {
        if (!MetricsManager.enabled) {
            return;
        }
        rspIndexCardinality.sample(rb.getCardinality());
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
                rspTwoValuesContainerCount);
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
        sortedRangesIndexCardinality.sample(sr.getCardinality());
        final long allocated = sr.bytesAllocated();
        sortedRangesIndexBytesAllocated.sample(allocated);
        sortedRangesIndexBytesUnused.sample(allocated - sr.bytesUsed());
    }

    public void sampleEmpty() {
        if (!MetricsManager.enabled) {
            return;
        }
        emptyCount.sample(1);
    }
}
