/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;

import static io.deephaven.util.SafeCloseable.closeAll;

/**
 * Static functions used by Typed Hashers.
 */
public class TypedHasherUtil {
    public static ColumnSource.GetContext[] makeGetContexts(ColumnSource<?>[] sources, final SharedContext sharedState,
            int chunkSize) {
        final ColumnSource.GetContext[] contexts = new ColumnSource.GetContext[sources.length];
        for (int ii = 0; ii < sources.length; ++ii) {
            contexts[ii] = sources[ii].makeGetContext(chunkSize, sharedState);
        }
        return contexts;
    }

    public static void getKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts,
            Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getChunk(contexts[ii], rowSequence);
        }
    }

    public static void getPrevKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts,
            Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getPrevChunk(contexts[ii], rowSequence);
        }
    }

    @FunctionalInterface
    public interface ProbeHandler {
        void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks);
    }

    @FunctionalInterface
    public interface BuildHandler {
        void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks);
    }

    public static class BuildOrProbeContext implements Context {
        public final int chunkSize;
        public final SharedContext sharedContext;
        public final ChunkSource.GetContext[] getContexts;

        public BuildOrProbeContext(ColumnSource<?>[] buildSources, int chunkSize) {
            Assert.gtZero(chunkSize, "chunkSize");
            this.chunkSize = chunkSize;
            if (buildSources.length > 1) {
                sharedContext = SharedContext.makeSharedContext();
            } else {
                sharedContext = null;
            }
            getContexts = makeGetContexts(buildSources, sharedContext, chunkSize);
        }

        public void resetSharedContexts() {
            if (sharedContext != null) {
                sharedContext.reset();
            }
        }

        public void closeSharedContexts() {
            if (sharedContext != null) {
                sharedContext.close();
            }
        }

        @Override
        public void close() {
            closeAll(getContexts);
            closeSharedContexts();
        }

        public static class BuildContext extends BuildOrProbeContext {
            public BuildContext(ColumnSource<?>[] buildSources, int chunkSize) {
                super(buildSources, chunkSize);
            }
        }

        public static class ProbeContext extends BuildOrProbeContext {
            public ProbeContext(ColumnSource<?>[] buildSources, int chunkSize) {
                super(buildSources, chunkSize);
            }
        }
    }
}
