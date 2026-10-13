//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.remotefilesource.RemoteFileSourceClassLoader;
import io.deephaven.engine.util.remotefilesource.RemoteFileSourceProvider;
import io.deephaven.plugin.type.ObjectTypeLookup.NoOp;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that an evaluation requiring a class cache reset fails when the cache cannot be cleared, rather than running
 * against bytecode compiled from sources that no longer apply, and that the reset is retried on the next evaluation.
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
            session.cleanup();
        }
        LivenessScopeStack.pop(livenessScope);
        livenessScope.release();
        livenessScope = null;
    }

    @Test
    public void testUnclearableClassCacheFailsTheEvaluationAndIsRetried() throws IOException {
        // A cache directory that no longer exists cannot be cleared, whatever the platform or user
        final File cacheDirectory = session.classCacheDirectory;
        FileUtils.deleteRecursively(cacheDirectory);
        assertFalse(cacheDirectory.exists());

        final RemoteFileSourceProvider provider =
                resourceName -> CompletableFuture.failedFuture(new FileNotFoundException(resourceName));
        final List<String> resourcePaths = List.of("test/notebook/Whatever.groovy");

        // A dirty declaration requires a reset, which cannot clear the missing cache. The failure is reported to the
        // user like a script error, and the script does not run.
        RemoteFileSourceClassLoader.getInstance().declareRemoteSources(provider, resourcePaths, true);
        final ScriptSession.Changes failed = session.evaluateScript("x = 1");
        assertNotNull("Expected the evaluation to fail while the class cache cannot be cleared", failed.error);
        assertNull("The script should not run when preparation fails",
                session.getQueryScope().readParamValue("x", null));

        // Once the cache is clearable again, the next evaluation must still reset, even though the same client
        // redeclaring unchanged sources would not on its own require one
        assertTrue(cacheDirectory.mkdirs());
        final File staleClass = new File(cacheDirectory, "Stale.class");
        assertTrue(staleClass.createNewFile());

        RemoteFileSourceClassLoader.getInstance().declareRemoteSources(provider, resourcePaths, false);
        final ScriptSession.Changes retried = session.evaluateScript("x = 1");
        retried.throwIfError();
        assertFalse("The failed reset should be retried, clearing the stale class", staleClass.exists());
        assertEquals(1, (int) session.getQueryScope().<Integer>readParamValue("x"));
    }
}
