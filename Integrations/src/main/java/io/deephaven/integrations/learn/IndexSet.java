package io.deephaven.integrations.learn;

public class IndexSet {

    int current;
    final int maxSize;
    long[] idx;

    public IndexSet(int maxSize) {
        current = -1;
        this.maxSize = maxSize;
        this.idx = new long[maxSize];
    }

    public int size() {
        return this.current + 1;
    }

    public long getItem(int i) throws Exception {
        if (i >= this.size()) {
            throw new Exception("Index out of bounds.");
        }
        return this.idx[i];
    }

    public void clear() {
        this.current = -2;
        this.idx = null;
    }

    public void add(long kk) throws Exception {
        if (this.current == this.idx.length) {
            throw new Exception("Adding more indices than can fit.");
        }
        this.current += 1;
        this.idx[this.current] = kk;
    }

    public boolean isFull() {
        return this.size() >= this.idx.length;
    }

}
