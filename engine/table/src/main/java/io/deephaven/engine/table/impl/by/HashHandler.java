package io.deephaven.engine.table.impl.by;

/**
 * Callback from generated hash table code.
 */
public interface HashHandler {
    /**
     * Called after a value is inserted into the main hash table
     * 
     * @param tableLocation location of insertion
     * @param chunkPosition the position in the chunk
     */
    void doMainInsert(int tableLocation, int chunkPosition);

    /**
     * Called after a value is found in the main hash table
     * 
     * @param tableLocation location in the table
     * @param chunkPosition the position in the chunk
     */
    void doMainFound(int tableLocation, int chunkPosition);

    /**
     * Called after a value is inserted into the overflow hash table
     * 
     * @param overflowLocation location of insertion
     * @param chunkPosition the position in the chunk
     */
    void doOverflowInsert(int overflowLocation, int chunkPosition);

    /**
     * Called after a value is found in the overflow hash table
     * 
     * @param overflowLocation location in the overflow table
     * @param chunkPosition the position in the chunk
     */
    void doOverflowFound(int overflowLocation, int chunkPosition);

    /**
     * Called during rehash when a value is moved in the main table
     * 
     * @param oldTableLocation the old location in the main table
     * @param newTableLocation the new location in the main table
     */
    void doMoveMain(int oldTableLocation, int newTableLocation);

    /**
     * Called during rehash when a value is promoted from overflow to the main table
     * 
     * @param overflowLocation the old location in the overflow table
     * @param mainInsertLocation the new location in the main table
     */
    void doPromoteOverflow(int overflowLocation, int mainInsertLocation);

    /**
     * Called when the next chunk of data is being processed in build or probe.
     * 
     * @param size the size of the next chunk
     */
    void onNextChunk(int size);

    /**
     * Called during probe when a value is not found in main or overflow.
     * 
     * @param chunkPosition the position in the chunk
     */
    void doMissing(int chunkPosition);

    /**
     * When building, this operation is never invoked. Extending this class makes it simpler to have a build only
     * callback.
     */
    abstract class BuildHandler implements HashHandler {
        @Override
        public void doMissing(int chunkPosition) {
            throw new IllegalStateException();
        }
    }

    /**
     * When probing, these operations are never invoked. Extending this class makes it simpler to have a probe only
     * callback.
     */
    abstract class ProbeHandler implements HashHandler {
        @Override
        final public void doMainInsert(int tableLocation, int chunkPosition) {
            throw new IllegalStateException();
        }

        @Override
        final public void doOverflowInsert(int overflowLocation, int chunkPosition) {
            throw new IllegalStateException();
        }

        @Override
        final public void doMoveMain(int oldTableLocation, int newTableLocation) {
            throw new IllegalStateException();
        }

        @Override
        final public void doPromoteOverflow(int overflowLocation, int mainInsertLocation) {
            throw new IllegalStateException();
        }
    }
}
