package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.TestSourceSink;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Random;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_CHAR;
// endregion boxing imports

import static junit.framework.TestCase.*;

public class TestCharacterSparseArraySource extends AbstractCharacterColumnSourceTest {
    @NotNull
    @Override
    CharacterSparseArraySource makeTestSource() {
        return new CharacterSparseArraySource();
    }
}
