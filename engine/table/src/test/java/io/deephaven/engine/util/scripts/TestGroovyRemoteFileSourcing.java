//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.scripts;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.GroovyDeephavenSession;
import io.deephaven.engine.util.RemoteFileSourceClassLoader;
import io.deephaven.engine.util.RemoteFileSourceProvider;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.plugin.type.ObjectTypeLookup.NoOp;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Tests for Groovy remote file sourcing in isolation. Unlike {@link TestGroovyDeephavenSession}, this test does NOT
 * hold a persistent ExecutionContext open via {@code session.getExecutionContext().open()} in setup. This more closely
 * matches production where {@code evaluateScript} is called without an outer EC wrapper — each call to
 * {@link io.deephaven.engine.util.AbstractScriptSession#evaluateScript} manages its own EC lifecycle internally via
 * {@code .apply()}.
 */
public class TestGroovyRemoteFileSourcing {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private LivenessScope livenessScope;
    private GroovyDeephavenSession session;

    private final Map<String, String> remoteSources = new HashMap<>();
    private final AtomicBoolean providerDirty = new AtomicBoolean(false);
    private RemoteFileSourceProvider provider;

    @Before
    public void setup() throws IOException {
        livenessScope = new LivenessScope();
        LivenessScopeStack.push(livenessScope);
        final ExecutionContext context = ExecutionContext.getContext();
        session = GroovyDeephavenSession.of(
                context.getUpdateGraph(), context.getOperationInitializer(), NoOp.INSTANCE,
                GroovyDeephavenSession.RunScripts.none());

        provider = createProvider();
        RemoteFileSourceClassLoader.getInstance().registerProvider(provider);
    }

    @After
    public void teardown() {
        RemoteFileSourceClassLoader.getInstance().unregisterProvider(provider);
        session.cleanup();
        LivenessScopeStack.pop(livenessScope);
        livenessScope.release();
        livenessScope = null;
    }

    private RemoteFileSourceProvider createProvider() {
        return new RemoteFileSourceProvider() {
            @Override
            public boolean canSourceResource(String resourceName) {
                return remoteSources.containsKey(resourceName);
            }

            @Override
            public boolean isActive() {
                return true;
            }

            @Override
            public boolean hasConfiguredResources() {
                return !remoteSources.isEmpty();
            }

            @Override
            public boolean isDirty() {
                return providerDirty.get();
            }

            @Override
            public CompletableFuture<byte[]> requestResource(String resourceName) {
                String source = remoteSources.get(resourceName);
                return source != null
                        ? CompletableFuture.completedFuture(source.getBytes())
                        : CompletableFuture.completedFuture(null);
            }
        };
    }

    private static final String PATH_ON_CLASSPATH = "test/notebook/MyDependency.groovy";
    private static final String SCRIPT_IMPORT_ON_CLASSPATH = entrypointScript("MyDependency");

    private static final String PATH_REMOTE_ONLY = "test/notebook/RemoteOnly.groovy";
    private static final String SCRIPT_IMPORT_REMOTE_ONLY = entrypointScript("RemoteOnly");

    private static final String REMOTE_SOURCE = remoteClassSource("REMOTE", 999);
    private static final String REMOTE_SOURCE_V2 = remoteClassSource("REMOTE_V2", 777);
    private static final String REMOTE_ONLY_SOURCE = remoteClassSource("REMOTE_ONLY", 555);

    private static String entrypointScript(String className) {
        return "import io.deephaven.engine.context.ExecutionContext\n" +
                "import test.notebook." + className + "\n\n" +
                "ExecutionContext.getContext().getQueryLibrary().importClass(" + className + ".class)\n\n" +
                "testTable = emptyTable(1).updateView(\n" +
                "    \"Version = " + className + ".getVersion()\",\n" +
                "    \"Value = " + className + ".getValue()\",\n" +
                "    \"SourceViaClass = " + className + ".getSourceViaClass()\"\n" +
                ")\n";
    }

    private static String remoteClassSource(String version, int value) {
        return "package test.notebook\n\n" +
                "static String getVersion() {\n" +
                "    return \"" + version + "\"\n" +
                "}\n\n" +
                "static int getValue() {\n" +
                "    return " + value + "\n" +
                "}\n\n" +
                "class Inner {\n" +
                "    final String source = \"" + version + "\"\n" +
                "    String getSource() {\n" +
                "        return source\n" +
                "    }\n" +
                "}\n\n" +
                "static String getSourceViaClass() {\n" +
                "    new Inner().getSource()\n" +
                "}";
    }

    /**
     * Configure the provider state, evaluate the entrypoint script, and assert the expected Helper values.
     *
     * @param step description for assertion messages
     * @param script the script to evaluate
     * @param remoteSourceMap the remote sources to provide (resource path → source content)
     * @param isDirty whether the provider should be marked dirty
     * @param expectedVersion expected Version and SourceViaClass value
     * @param expectedValue expected Value
     */
    private void evaluateAndAssertHelper(String step, String script,
            Map<String, String> remoteSourceMap, boolean isDirty,
            String expectedVersion, int expectedValue) {
        remoteSources.clear();
        remoteSources.putAll(remoteSourceMap);
        providerDirty.set(isDirty);

        ScriptSession.Changes c = session.evaluateScript(script);
        c.throwIfError();

        Table t = session.getQueryScope().readParamValue("testTable");
        assertEquals(step + ": Version", expectedVersion, (String) t.getColumnSource("Version").get(0));
        assertEquals(step + ": Value", expectedValue, t.getColumnSource("Value").getInt(0));
        assertEquals(step + ": SourceViaClass", expectedVersion,
                (String) t.getColumnSource("SourceViaClass").get(0));
    }

    @Test
    public void testServerClassWorks() {
        evaluateAndAssertHelper(
                "server baseline",
                SCRIPT_IMPORT_ON_CLASSPATH,
                Map.of(),
                false,
                "SERVER",
                100);
    }

    @Test
    public void testRemoteOverridesServer() {
        evaluateAndAssertHelper(
                "remote overrides server",
                SCRIPT_IMPORT_ON_CLASSPATH,
                Map.of(PATH_ON_CLASSPATH, REMOTE_SOURCE),
                true,
                "REMOTE",
                999);
    }

    @Test
    public void testServerToRemoteAndBack() {
        evaluateAndAssertHelper(
                "step 1: server",
                SCRIPT_IMPORT_ON_CLASSPATH,
                Map.of(),
                false,
                "SERVER",
                100);

        evaluateAndAssertHelper(
                "step 2: server→remote",
                SCRIPT_IMPORT_ON_CLASSPATH,
                Map.of(PATH_ON_CLASSPATH, REMOTE_SOURCE),
                true,
                "REMOTE",
                999);

        evaluateAndAssertHelper(
                "step 3: remote→server",
                SCRIPT_IMPORT_ON_CLASSPATH,
                Map.of(),
                true,
                "SERVER",
                100);
    }

    @Test
    public void testRemoteToServer() {
        evaluateAndAssertHelper(
                "step 1: remote",
                SCRIPT_IMPORT_ON_CLASSPATH,
                Map.of(PATH_ON_CLASSPATH, REMOTE_SOURCE),
                true,
                "REMOTE",
                999);

        evaluateAndAssertHelper(
                "step 2: remote→server",
                SCRIPT_IMPORT_ON_CLASSPATH,
                Map.of(),
                true,
                "SERVER",
                100);
    }

    @Test
    public void testIsDirtyClearsCacheWithinRemote() {
        evaluateAndAssertHelper(
                "step 1: remote v1",
                SCRIPT_IMPORT_ON_CLASSPATH,
                Map.of(PATH_ON_CLASSPATH, REMOTE_SOURCE),
                true,
                "REMOTE",
                999);

        evaluateAndAssertHelper(
                "step 2: remote v2",
                SCRIPT_IMPORT_ON_CLASSPATH,
                Map.of(PATH_ON_CLASSPATH, REMOTE_SOURCE_V2),
                true,
                "REMOTE_V2",
                777);
    }

    @Test
    public void testRemoteOnlyClassRemoved() {
        // RemoteOnly has no server fallback on the classpath — it only exists via the remote provider

        // Step 1: Remote-only class available
        evaluateAndAssertHelper(
                "remote-only available",
                SCRIPT_IMPORT_REMOTE_ONLY,
                Map.of(PATH_REMOTE_ONLY, REMOTE_ONLY_SOURCE),
                true,
                "REMOTE_ONLY",
                555);

        // Step 2: Remove remote sources — no server fallback exists, should fail
        remoteSources.clear();
        providerDirty.set(true);

        ScriptSession.Changes c2 = session.evaluateScript(SCRIPT_IMPORT_REMOTE_ONLY);
        assertTrue("Script should fail when remote-only class is removed", c2.error != null);
    }
}
