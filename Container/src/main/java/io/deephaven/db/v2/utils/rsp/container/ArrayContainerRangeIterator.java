package io.deephaven.db.v2.utils.rsp.container;


final class ArrayContainerRangeIterator implements SearchRangeIterator {
    private final ArrayContainer parent;

    private int nextPos;
    private int start;
    private int end;

    public ArrayContainerRangeIterator(final ArrayContainer p, final int initialSkipCount) {
        parent = p;
        nextPos = initialSkipCount;
    }

    public ArrayContainerRangeIterator(final ArrayContainerRangeIterator other) {
        nextPos = other.nextPos;
        start = other.start;
        end = other.end;
        parent = other.parent;

    }

    public ArrayContainerRangeIterator copy() {
        return new ArrayContainerRangeIterator(this);
    }

    @Override
    public boolean hasNext() {
        return nextPos < parent.cardinality;
    }

    @Override
    public int start() {
        return start;
    }

    @Override
    public int end() {
        return end;
    }

    @Override
    public void next() {
        start = ContainerUtil.toIntUnsigned(parent.content[nextPos++]);
        end = start + 1;
        while (nextPos < parent.cardinality) {
            int v = ContainerUtil.toIntUnsigned(parent.content[nextPos]);
            if (v != end) {
                break;
            }
            ++nextPos;
            end = v + 1;
        }
    }

    /**
     * Set the current iterator range based on the index idx and value v provided. Look for a range
     * starting at the value at index idx. The provided value v is to the left or inside that range,
     * never to the right. Set the end of the current iterator range to the end of the range at idx.
     * Set the current iterator range start to v if v is inside the range, or the start of the range
     * at idx otherwise.
     *
     * @param v A value to the left or inside the range at idx
     * @param idx a valid index inside our contents array.
     */
    private void setRangeBoundariesFor(final int v, final int idx) {
        // find actual range boundaries.
        int vi = ContainerUtil.toIntUnsigned(parent.content[idx]);
        end = vi + 1;
        int iend = idx;
        for (int j = idx + 1; j < parent.cardinality; ++j) {
            final int vj = ContainerUtil.toIntUnsigned(parent.content[j]);
            if (vj - vi > j - idx) {
                break;
            } else {
                iend = j;
                end = vj + 1;
            }
        }
        start = Math.max(v, vi);
        nextPos = iend + 1;
    }

    @Override
    public boolean advance(final int v) {
        if (nextPos > 0 && end > v) {
            return true;
        }
        int i = ContainerUtil.unsignedBinarySearch(parent.content, nextPos, parent.cardinality,
            ContainerUtil.lowbits(v));
        if (i < 0) {
            i = -i - 1;
        }
        if (i >= parent.cardinality) {
            nextPos = parent.cardinality;
            return false;
        }
        setRangeBoundariesFor(v, i);
        return true;
    }

    @Override
    public boolean search(final ContainerUtil.TargetComparator comp) {
        if (nextPos == 0) {
            if (!hasNext()) {
                return false;
            }
            next();
        }
        int c = comp.directionFrom(end - 1);
        if (c <= 0) {
            if (c == 0) {
                start = end - 1;
                return true;
            }
            if (start == end - 1) {
                return false;
            }
            c = comp.directionFrom(start);
            if (c < 0) {
                return false;
            }
            if (c == 0) {
                return true;
            }
            int j = ContainerUtil.rangeSearch(start + 1, end - 1, comp);
            if (j == -1) {
                return true;
            }
            start = j;
            return true;
        }
        int i = ContainerUtil.search(parent.content, nextPos, parent.cardinality, comp);
        if (i < 0) {
            setRangeBoundariesFor(start, nextPos - 1);
            return true;
        }
        setRangeBoundariesFor(parent.content[i], i);
        return true;
    }
}
