package io.deephaven.db.v2.utils.rsp.container;

import static io.deephaven.db.v2.utils.rsp.container.ContainerUtil.toIntUnsigned;

final class RunContainerRangeIterator implements SearchRangeIterator {
    private final RunContainer parent;

    private int pos;
    private int start;
    private int end; // end == -1 marks a just initialized iterator for which hasNext()/next() have
                     // not been called yet.

    RunContainerRangeIterator(final RunContainer p) {
        this(p, 0);
    }

    RunContainerRangeIterator(final RunContainer p, final int initialSkipCount) {
        pos = 0;
        parent = p;
        end = -1;
        final int numRuns = parent.numberOfRuns();
        if (numRuns == 0) {
            return;
        }
        if (initialSkipCount == 0) {
            start = runStart(pos);
            return;
        }
        int remaining = initialSkipCount;
        while (true) {
            int runSize = toIntUnsigned(parent.getLength(pos)) + 1;
            if (remaining < runSize) {
                start = runStart(pos) + remaining;
                break;
            }
            remaining -= runSize;
            ++pos;
            if (pos >= numRuns) {
                break;
            }
        }
    }

    RunContainerRangeIterator(final RunContainerRangeIterator other) {
        this.pos = other.pos;
        this.start = other.start;
        this.end = other.end;
        this.parent = other.parent;
    }

    public RunContainerRangeIterator copy() {
        return new RunContainerRangeIterator(this);
    }

    @Override
    public boolean hasNext() {
        if (end == -1) {
            return pos < parent.nbrruns;
        }
        return pos + 1 < parent.nbrruns;
    }

    private int runStart(final int i) {
        return toIntUnsigned(parent.getValue(i));
    }

    @Override
    public int start() {
        return start;
    }

    private int runLast(final int i) {
        return runLast(runStart(i), i);
    }

    private int runLast(final int runStart, final int i) {
        return runStart + toIntUnsigned(parent.getLength(i));
    }

    @Override
    public int end() {
        return end;
    }

    @Override
    public void next() {
        if (end == -1) {
            end = runLast(pos) + 1;
            return;
        }
        ++pos;
        start = runStart(pos);
        end = runLast(start, pos) + 1;
    }

    @Override
    public int next(final short[] buffer, final int offset, final int maxRanges) {
        int count = 0;
        if (end == -1 && maxRanges > 0) {
            buffer[offset] = (short) start;
            final int e = runLast(pos);
            buffer[offset + 1] = (short) e;
            ++count;
            if (pos + 1 >= parent.nbrruns || maxRanges < 2) {
                end = e + 1;
                return 1;
            }
        } else if (maxRanges <= 0) {
            return 0;
        }
        int s;
        int e;
        do {
            ++pos;
            s = runStart(pos);
            e = runLast(s, pos);
            buffer[offset + 2 * count] = (short) s;
            buffer[offset + 2 * count + 1] = (short) e;
            ++count;
        } while (count < maxRanges && pos + 1 < parent.nbrruns);
        start = s;
        end = e + 1;
        return count;
    }

    @Override
    public boolean advance(final int v) {
        if (end == -1) {
            if (!hasNext()) {
                return false;
            }
            next();
        }
        if (end - 1 >= v) {
            start = Math.max(start, v);
            return true;
        }
        int left = Math.max(0, pos);
        int right = parent.nbrruns - 1;
        if (runLast(right) < v) {
            pos = parent.nbrruns;
            return false;
        }
        // binary search over end elements.
        while (true) {
            pos = (left + right) / 2;
            if (runLast(pos) < v) {
                left = pos + 1;
            } else {
                if (right == pos) {
                    final int runStart = runStart(pos);
                    start = Math.max(runStart, v);
                    end = runLast(runStart, pos) + 1;
                    return true;
                }
                right = pos;
            }
        }
    }

    @Override
    public boolean search(final ContainerUtil.TargetComparator comp) {
        if (end == -1) {
            if (!hasNext()) {
                return false;
            }
            next();
        }
        if (comp.directionFrom(start) < 0) {
            return false;
        }
        int posStart = start;
        // we know comp.directionFrom(start) >= 0.
        int right = parent.nbrruns - 1;
        // 32 in the next line matches a cache line in shorts.
        // Note that since we don't know where in cache line boundaries
        // pos and right are, their range may span at most 2.
        while (pos + 32 < right) {
            final int mid = (pos + right) / 2;
            final int midStart = runStart(mid);
            final int c = comp.directionFrom(midStart);
            if (c < 0) {
                right = mid - 1;
                continue;
            }
            pos = mid;
            posStart = midStart;
        }
        // we finish the job with a sequential search.
        while (pos < right) {
            final int posNext = pos + 1;
            final int posNextStart = runStart(posNext);
            final int c = comp.directionFrom(posNextStart);
            if (c < 0) {
                break;
            }
            pos = posNext;
            posStart = posNextStart;
        }
        end = runLast(pos) + 1;
        start = ContainerUtil.rangeSearch(posStart, end, comp);
        assert (start != -1);
        return true;
    }
}
