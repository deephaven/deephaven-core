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
import static org.junit.Assert.assertTrue;

/**
 * Tests that an evaluation whose remote sources changed starts from an empty class cache, rather than running against
 * bytecode compiled from sources that no longer apply.
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
    public void testDirtyDeclarationClearsTheClassCache() throws IOException {
        final File cacheDirectory = session.classCacheDirectory;
        final File staleClass = new File(cacheDirectory, "Stale.class");
        final File staleNestedClass = new File(cacheDirectory, "pkg/Nested.class");
        assertTrue(staleNestedClass.getParentFile().mkdirs());
        assertTrue(staleClass.createNewFile());
        assertTrue(staleNestedClass.createNewFile());

        declareDirty();
        session.evaluateScript("x = 1").throwIfError();

        assertFalse("Cached classes compiled from earlier sources should be cleared", staleClass.exists());
        assertFalse("Cached classes in nested packages should be cleared", staleNestedClass.exists());
        assertTrue("The cache directory must remain for Groovy and the QueryCompiler", cacheDirectory.isDirectory());
        assertEquals(1, (int) session.getQueryScope().<Integer>readParamValue("x"));
    }

    @Test
    public void testMissingClassCacheIsRecreated() {
        final File cacheDirectory = session.classCacheDirectory;
        FileUtils.deleteRecursively(cacheDirectory);
        assertFalse(cacheDirectory.exists());

        declareDirty();
        session.evaluateScript("x = 1").throwIfError();

        assertTrue("The cache directory should exist again after the evaluation", cacheDirectory.isDirectory());
        assertEquals(1, (int) session.getQueryScope().<Integer>readParamValue("x"));
    }

    /**
     * Declares remote sources as changed, which requires the next evaluation to reset the class cache.
     */
    private static void declareDirty() {
        final RemoteFileSourceProvider provider =
                resourceName -> CompletableFuture.failedFuture(new FileNotFoundException(resourceName));
        RemoteFileSourceClassLoader.getInstance().declareRemoteSources(
                provider, List.of("test/notebook/Whatever.groovy"), true);
    }
}
