package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.util.SafeCloseable;

import java.util.ArrayList;

public class ChunkList implements SafeCloseable {
    // rows in position space for each chunk
    public final ArrayList<Long> startIndex = new ArrayList<>();
    public final ArrayList<Long> endIndex = new ArrayList<>();
    public final ArrayList<Chunk<Values>> chunks = new ArrayList<>();

    public void addChunk(Chunk<Values> nextChunk, long start, long end) {
        startIndex.add(start);
        endIndex.add(end);
        chunks.add(nextChunk);
    }

    public void addChunk(Chunk<Values> nextChunk) {
        final long start;
        final long end;

        if (endIndex.isEmpty()) {
            start = 0L;
            end = (long)nextChunk.size() - 1;
        } else {
            start = lastEndIndex() + 1;
            end = start + (long)nextChunk.size() - 1;
        }

        addChunk(nextChunk, start, end);
    }

    public int size() {
        return startIndex.size();
    }

    public Long chunkSize(int ci) {
        if (ci < startIndex.size() && ci < endIndex.size()) {
            return endIndex.get(ci) - startIndex.get(ci);
        }
        return null;
    }

    public Chunk<Values> firstChunk() {
        if (chunks.size() == 0) {
            return null;
        }
        return chunks.get(0);
    }

    public Chunk<Values> lastChunk() {
        if (chunks.size() == 0) {
            return null;
        }
        return chunks.get(chunks.size() -1);
    }

    public Long lastStartIndex() {
        if (startIndex.size() == 0) {
            return null;
        }
        return startIndex.get(startIndex.size() -1);
    }

    public Long lastEndIndex() {
        if (endIndex.size() == 0) {
            return null;
        }
        return endIndex.get(endIndex.size() -1);
    }

    public Long lastChunkSize() {
        return chunkSize(endIndex.size() -1);
    }


    @Override
    public void close() {
        for (Chunk<Values> chunk : chunks){
            if (chunk instanceof PoolableChunk) {
                ((PoolableChunk) chunk).close();
            }
        }
    }
}