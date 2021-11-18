package io.deephaven.engine.table.impl.locations.parquet.topage;

import io.deephaven.engine.page.ChunkPageFactory;
import io.deephaven.engine.vector.Vector;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.vector.VectorFactory;
import io.deephaven.engine.table.impl.sources.StringSetImpl;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.ChunkType;
import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.parquet.ColumnPageReader;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * This provides a translation layer from the parquet results into the appropriately typed Chunk's.
 */
public interface ToPage<ATTR extends Attributes.Any, RESULT> {

    /**
     * @return The native type for the elements of the arrays produced by this object.
     */
    @NotNull
    Class<?> getNativeType();

    /**
     * @return The native type for the elements of engine arrays produced by this object.
     * @apiNote
     */
    @NotNull
    default Class<?> getNativeComponentType() {
        return getNativeType();
    }

    /**
     * @return The chunk type used to wrap the arrays produced by this object.
     */
    @NotNull
    ChunkType getChunkType();

    /**
     * @return The null value stored in the elements of the arrays produced by thus object.
     */
    default Object nullValue() {
        return null;
    }

    /**
     * @return Gets the result from the columnPageReader.
     */
    default Object getResult(ColumnPageReader columnPageReader) throws IOException {
        return columnPageReader.materialize(nullValue());
    }

    /**
     * @return Produce the array of values from the result
     */
    default RESULT convertResult(Object result) {
        // noinspection unchecked
        return (RESULT) result;
    }

    /**
     * @return the method to create a Vector from RESULT.
     */
    default Vector makeVector(RESULT result) {
        return VectorFactory.forElementType(getNativeType()).vectorWrap(result);
    }

    /**
     * Produce the appropriately typed chunk page for the page read by the columnPageReader. The is the expected entry
     * point for the ColumnChunkPageStore.
     */
    @NotNull
    @FinalDefault
    default ChunkPage<ATTR> toPage(long offset, ColumnPageReader columnPageReader, long mask)
            throws IOException {
        return ChunkPageFactory.forChunkType(getChunkType())
                .pageWrap(offset, convertResult(getResult(columnPageReader)), mask);
    }

    /**
     * @return the dictionary stored for this column, if one exists, otherwise null.
     */
    default Chunk<ATTR> getDictionaryChunk() {
        return null;
    }

    /**
     * @return an object implementing ToPage which will read the integral Dictionary Indices when there's a dictionary
     *         for this column (as opposed to the values, which this object's toPage will return). This will return
     *         null iff {@link #getDictionaryChunk()} returns null.
     * @apiNote null iff {@link #getDictionaryChunk()} is null.
     */
    default ToPage<Attributes.DictionaryKeys, long[]> getDictionaryKeysToPage() {
        return null;
    }

    /**
     * @return an reverse lookup map of the dictionary.
     * @apiNote null iff {@link #getDictionaryChunk()} is null.
     */
    default StringSetImpl.ReversibleLookup getReversibleLookup() {
        return null;
    }

    abstract class Wrap<ATTR extends Attributes.Any, INNER_RESULT, OUTER_RESULT>
            implements ToPage<ATTR, OUTER_RESULT> {

        final ToPage<ATTR, INNER_RESULT> toPage;

        Wrap(ToPage<ATTR, INNER_RESULT> toPage) {
            this.toPage = toPage;
        }

        public Object nullValue() {
            return toPage.nullValue();
        }

        @NotNull
        @Override
        public Object getResult(ColumnPageReader columnPageReader) throws IOException {
            return toPage.getResult(columnPageReader);
        }

        @Override
        public abstract OUTER_RESULT convertResult(Object object);


        @Override
        public Chunk<ATTR> getDictionaryChunk() {
            return toPage.getDictionaryChunk();
        }

        @Override
        public ToPage<Attributes.DictionaryKeys, long[]> getDictionaryKeysToPage() {
            return toPage.getDictionaryKeysToPage();
        }

        @Override
        public StringSetImpl.ReversibleLookup getReversibleLookup() {
            return toPage.getReversibleLookup();
        }
    }
}
