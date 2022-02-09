package io.deephaven.engine.table.impl.by;

public interface HashHandler {
    void doMainInsert(int tableLocation, int chunkPosition);

    void doMainFound(int tableLocation, int chunkPosition);

    void doOverflowFound(int overflowLocation, int chunkPosition);

    void doOverflowInsert(int overflowLocation, int chunkPosition);

    void moveMain(int oldTableLocation, int newTableLocation);

    void promoteOverflow(int overflowLocation, int mainInsertLocation);

    void nextChunk(int size);
}
