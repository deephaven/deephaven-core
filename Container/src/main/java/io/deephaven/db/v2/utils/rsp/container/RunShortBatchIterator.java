package io.deephaven.db.v2.utils.rsp.container;

import static io.deephaven.db.v2.utils.rsp.container.ContainerUtil.toIntUnsigned;

public class RunShortBatchIterator implements ContainerShortBatchIterator {
    private final RunContainer runs;

    private int run = 0;
    private int cursor = 0; // relative position inside the current run.

    public RunShortBatchIterator(final RunContainer runs, final int initialSkipCount) {
        this.runs = runs;
        final int numRuns = runs.numberOfRuns();
        int remaining = initialSkipCount;
        while (true) {
            int runSize = toIntUnsigned(runs.getLength(run)) + 1;
            if (remaining < runSize) {
                cursor = remaining;
                break;
            }
            remaining -= runSize;
            ++run;
            if (run >= numRuns) {
                break;
            }
        }
    }

    @Override
    public int next(final short[] buffer, final int offset, final int maxCount) {
        int count = 0;
        do {
            int runStart = toIntUnsigned(runs.getValue(run));
            int runLength = toIntUnsigned(runs.getLength(run));
            int chunkStart = runStart + cursor;
            int chunkEnd = chunkStart + Math.min(runLength - cursor, maxCount - count - 1);
            int chunk = chunkEnd - chunkStart + 1;
            for (int i = 0; i < chunk; ++i) {
                buffer[offset + count + i] = (short) (chunkStart + i);
            }
            count += chunk;
            if (runStart + runLength == chunkEnd) {
                ++run;
                cursor = 0;
            } else {
                cursor += chunk;
            }
        } while (count < maxCount && run != runs.numberOfRuns());
        return count;
    }

    @Override
    public boolean hasNext() {
        return run < runs.numberOfRuns();
    }

    @Override
    public boolean forEach(final ShortConsumer sc) {
        final int numRuns = runs.numberOfRuns();
        while (run < numRuns) {
            int runStart = toIntUnsigned(runs.getValue(run));
            int runLength = toIntUnsigned(runs.getLength(run));
            boolean wantMore = true;
            while (cursor <= runLength) {
                wantMore = sc.accept((short) (runStart + cursor));
                ++cursor;
                if (!wantMore) {
                    break;
                }
            }
            if (cursor > runLength) {
                ++run;
                cursor = 0;
            }
            if (!wantMore) {
                return false;
            }
        }
        return true;
    }
}
