package io.deephaven.engine.rowset.impl.rsp.container;

public class ArrayShortBatchIterator implements ContainerShortBatchIterator {
    private final ArrayContainer array;

    private int index;

    public ArrayShortBatchIterator(final ArrayContainer array, final int initialSkipCount) {
        this.array = array;
        index = initialSkipCount;
    }

    @Override
    public int next(final short[] buffer, final int offset, final int maxCount) {
        final int count = Math.min(maxCount, array.cardinality - index);
        System.arraycopy(array.content, index, buffer, offset, count);
        index += count;
        return count;
    }

    @Override
    public boolean hasNext() {
        return index < array.getCardinality();
    }

    @Override
    public boolean forEach(final ShortConsumer sc) {
        while (index < array.cardinality) {
            final boolean wantMore = sc.accept(array.content[index++]);
            if (!wantMore) {
                return false;
            }
        }
        return true;
    }
}
