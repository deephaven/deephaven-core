//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.plugin.type.ObjectTypeLookup.NoOp;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests that an evaluation requiring a class cache reset fails when the cache cannot be cleared, rather than running
 * against bytecode compiled from sources that no longer apply.
 */
public class TestGroovyClassCacheReset {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private LivenessScope livenessScope;
    private GroovyDeephavenSession session;

    @Before
    public void setup() throws IOException {
        livenessScope = new LivenessScope();
        LivenessScopeStack.push(livenessScope);
        final ExecutionContext context = ExecutionContext.getContext();
        session = GroovyDeephavenSession.of(
                context.getUpdateGraph(), context.getOperationInitializer(), NoOp.INSTANCE,
                GroovyDeephavenSession.RunScripts.none());
    }

    @After
    public void teardown() {
        if (session != null) {
            // Restore access so the session can delete its own cache directory
            session.classCacheDirectory.setReadable(true);
            session.cleanup();
        }
        LivenessScopeStack.pop(livenessScope);
        livenessScope.release();
        livenessScope = null;
    }

    @Test
    public void testUnclearableClassCacheFailsTheEvaluation() {
        final File cacheDirectory = session.classCacheDirectory;
        assumeTrue("Requires a filesystem that enforces read permissions",
                cacheDirectory.setReadable(false) && cacheDirectory.listFiles() == null);

        // A dirty declaration requires a reset, which cannot clear a cache it cannot read
        RemoteFileSourceClassLoader.getInstance().declareExecutionContext(
                resourceName -> CompletableFuture.completedFuture(null),
                List.of("test/notebook/Whatever.groovy"), true);

        try {
            session.evaluateScript("x = 1");
            fail("Expected the evaluation to fail while the class cache cannot be cleared");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("unable to list class cache directory"));
        }
    }
}
