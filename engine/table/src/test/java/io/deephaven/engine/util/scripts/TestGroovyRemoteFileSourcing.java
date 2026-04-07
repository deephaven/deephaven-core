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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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

    @Before
    public void setup() throws IOException {
        livenessScope = new LivenessScope();
        LivenessScopeStack.push(livenessScope);
        final ExecutionContext context = ExecutionContext.getContext();
        session = GroovyDeephavenSession.of(
                context.getUpdateGraph(), context.getOperationInitializer(), NoOp.INSTANCE,
                GroovyDeephavenSession.RunScripts.none());
        // NOTE: intentionally NOT calling session.getExecutionContext().open() here.
        // Production (ConsoleServiceGrpcImpl) does not hold a persistent outer EC — each
        // evaluateScript call manages its own EC via .apply() internally.
    }

    @After
    public void teardown() {
        session.cleanup();
        LivenessScopeStack.pop(livenessScope);
        livenessScope.release();
        livenessScope = null;
    }

    private String loadTestScript(String resourcePath) throws IOException {
        try (InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private RemoteFileSourceProvider createProvider(
            Map<String, String> remoteSources,
            AtomicBoolean providerActive,
            AtomicBoolean providerDirty) {
        return new RemoteFileSourceProvider() {
            @Override
            public boolean canSourceResource(String resourceName) {
                return remoteSources.containsKey(resourceName);
            }

            @Override
            public boolean isActive() {
                return providerActive.get();
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

    private static final String REMOTE_HELPER = "package test.notebook\n\n" +
            "return \"Helper\"\n\n" +
            "static String getVersion() {\n" +
            "    return \"REMOTE\"\n" +
            "}\n\n" +
            "static int getValue() {\n" +
            "    return 999\n" +
            "}\n\n" +
            "class HelperClass {\n" +
            "    final String source = \"REMOTE\"\n" +
            "    String getSource() {\n" +
            "        return source\n" +
            "    }\n" +
            "}\n\n" +
            "static String getSourceViaClass() {\n" +
            "    new HelperClass().getSource()\n" +
            "}";

    @Test
    public void testServerClassWorks() throws IOException {
        // Scenario 1: Server Works Baseline - verify server Helper class from classpath works
        String script = loadTestScript("/test-scripts/remote-test-entrypoint.groovy");

        ScriptSession.Changes c = session.evaluateScript(script);
        c.throwIfError();

        Table t = session.getQueryScope().readParamValue("testTable");
        assertEquals("SERVER", (String) t.getColumnSource("Version").get(0));
        assertEquals(100, t.getColumnSource("Value").getInt(0));
        assertEquals("SERVER", (String) t.getColumnSource("SourceViaClass").get(0));
    }

    @Test
    public void testRemoteOverridesServer() throws IOException {
        // Scenario 2: Remote Override Takes Priority
        String script = loadTestScript("/test-scripts/remote-test-entrypoint.groovy");

        final Map<String, String> remoteSources = new HashMap<>();
        final AtomicBoolean providerActive = new AtomicBoolean(true);
        final AtomicBoolean providerDirty = new AtomicBoolean(true);

        remoteSources.put("test/notebook/Helper.groovy", REMOTE_HELPER);

        RemoteFileSourceProvider provider = createProvider(remoteSources, providerActive, providerDirty);
        RemoteFileSourceClassLoader cl = RemoteFileSourceClassLoader.getInstance();
        try {
            cl.registerProvider(provider);

            ScriptSession.Changes c = session.evaluateScript(script);
            c.throwIfError();

            providerDirty.set(false);

            Table t = session.getQueryScope().readParamValue("testTable");
            assertEquals("REMOTE", (String) t.getColumnSource("Version").get(0));
            assertEquals(999, t.getColumnSource("Value").getInt(0));
            assertEquals("REMOTE", (String) t.getColumnSource("SourceViaClass").get(0));
        } finally {
            cl.unregisterProvider(provider);
        }
    }

    @Test
    public void testServerToRemoteAndBack() throws IOException {
        // Scenario 3: Server → Remote → Back to Server
        String script = loadTestScript("/test-scripts/remote-test-entrypoint.groovy");

        final Map<String, String> remoteSources = new HashMap<>();
        final AtomicBoolean providerActive = new AtomicBoolean(false);
        final AtomicBoolean providerDirty = new AtomicBoolean(false);

        RemoteFileSourceProvider provider = createProvider(remoteSources, providerActive, providerDirty);
        RemoteFileSourceClassLoader cl = RemoteFileSourceClassLoader.getInstance();

        try {
            cl.registerProvider(provider);

            // Step 1: Provider is registered but inactive — server Helper is used
            ScriptSession.Changes c1 = session.evaluateScript(script);
            c1.throwIfError();

            Table t1 = session.getQueryScope().readParamValue("testTable");
            assertEquals("SERVER", (String) t1.getColumnSource("Version").get(0));
            assertEquals(100, t1.getColumnSource("Value").getInt(0));
            assertEquals("SERVER", (String) t1.getColumnSource("SourceViaClass").get(0));

            // Step 2: Activate remote and verify remote version is used
            remoteSources.put("test/notebook/Helper.groovy", REMOTE_HELPER);
            providerActive.set(true);
            providerDirty.set(true);

            ScriptSession.Changes c2 = session.evaluateScript(script);
            c2.throwIfError();

            providerDirty.set(false);

            Table t2 = session.getQueryScope().readParamValue("testTable");
            assertEquals("REMOTE", (String) t2.getColumnSource("Version").get(0));
            assertEquals(999, t2.getColumnSource("Value").getInt(0));
            assertEquals("REMOTE", (String) t2.getColumnSource("SourceViaClass").get(0));

            // Step 3: Deactivate provider and verify server version is used again
            providerActive.set(false);
            providerDirty.set(true);

            ScriptSession.Changes c3 = session.evaluateScript(script);
            c3.throwIfError();

            Table t3 = session.getQueryScope().readParamValue("testTable");
            assertEquals("SERVER", (String) t3.getColumnSource("Version").get(0));
            assertEquals(100, t3.getColumnSource("Value").getInt(0));
            assertEquals("SERVER", (String) t3.getColumnSource("SourceViaClass").get(0));
        } finally {
            cl.unregisterProvider(provider);
        }
    }

    @Test
    public void testRemoteThenUnrelatedScript() throws IOException {
        // Scenario 4: Remote → Deactivate → Use Helper Again (should fall back to SERVER)
        String script = loadTestScript("/test-scripts/remote-test-entrypoint.groovy");

        final Map<String, String> remoteSources = new HashMap<>();
        final AtomicBoolean providerActive = new AtomicBoolean(true);
        final AtomicBoolean providerDirty = new AtomicBoolean(true);

        remoteSources.put("test/notebook/Helper.groovy", REMOTE_HELPER);

        RemoteFileSourceProvider provider = createProvider(remoteSources, providerActive, providerDirty);
        RemoteFileSourceClassLoader cl = RemoteFileSourceClassLoader.getInstance();
        try {
            cl.registerProvider(provider);

            // Step 1: Use Helper from remote
            ScriptSession.Changes c1 = session.evaluateScript(script);
            c1.throwIfError();

            Table t1 = session.getQueryScope().readParamValue("testTable");
            assertEquals("REMOTE", (String) t1.getColumnSource("Version").get(0));
            assertEquals(999, t1.getColumnSource("Value").getInt(0));
            assertEquals("REMOTE", (String) t1.getColumnSource("SourceViaClass").get(0));

            providerDirty.set(false);

            // Step 2: Deactivate provider and use Helper again (should fall back to SERVER)
            providerActive.set(false);
            providerDirty.set(true);

            ScriptSession.Changes c2 = session.evaluateScript(script);
            c2.throwIfError();

            Table t2 = session.getQueryScope().readParamValue("testTable");
            assertEquals("SERVER", (String) t2.getColumnSource("Version").get(0));
            assertEquals(100, t2.getColumnSource("Value").getInt(0));
            assertEquals("SERVER", (String) t2.getColumnSource("SourceViaClass").get(0));
        } finally {
            cl.unregisterProvider(provider);
        }
    }

    @Test
    public void testIsDirtyClearsCacheWithinRemote() throws IOException {
        // Scenario 5: isDirty Clears Cache (Remote v1 → Remote v2)
        String script = loadTestScript("/test-scripts/remote-test-entrypoint.groovy");

        final Map<String, String> remoteSources = new HashMap<>();
        final AtomicBoolean providerActive = new AtomicBoolean(true);
        final AtomicBoolean providerDirty = new AtomicBoolean(true);

        String remoteHelperV2 = "package test.notebook\n\n" +
                "return \"Helper\"\n\n" +
                "static String getVersion() {\n" +
                "    return \"REMOTE_V2\"\n" +
                "}\n\n" +
                "static int getValue() {\n" +
                "    return 777\n" +
                "}\n\n" +
                "class HelperClass {\n" +
                "    final String source = \"REMOTE_V2\"\n" +
                "    String getSource() {\n" +
                "        return source\n" +
                "    }\n" +
                "}\n\n" +
                "static String getSourceViaClass() {\n" +
                "    new HelperClass().getSource()\n" +
                "}";

        remoteSources.put("test/notebook/Helper.groovy", REMOTE_HELPER);

        RemoteFileSourceProvider provider = createProvider(remoteSources, providerActive, providerDirty);
        RemoteFileSourceClassLoader cl = RemoteFileSourceClassLoader.getInstance();
        try {
            cl.registerProvider(provider);

            // Step 1: Use remote v1
            ScriptSession.Changes c1 = session.evaluateScript(script);
            c1.throwIfError();

            Table t1 = session.getQueryScope().readParamValue("testTable");
            assertEquals("REMOTE", (String) t1.getColumnSource("Version").get(0));
            assertEquals(999, t1.getColumnSource("Value").getInt(0));
            assertEquals("REMOTE", (String) t1.getColumnSource("SourceViaClass").get(0));

            providerDirty.set(false);

            // Step 2: Update to remote v2 and mark as dirty
            remoteSources.put("test/notebook/Helper.groovy", remoteHelperV2);
            providerDirty.set(true);

            ScriptSession.Changes c2 = session.evaluateScript(script);
            c2.throwIfError();

            Table t2 = session.getQueryScope().readParamValue("testTable");
            assertEquals("REMOTE_V2", (String) t2.getColumnSource("Version").get(0));
            assertEquals(777, t2.getColumnSource("Value").getInt(0));
            assertEquals("REMOTE_V2", (String) t2.getColumnSource("SourceViaClass").get(0));
        } finally {
            cl.unregisterProvider(provider);
        }
    }

    @Test
    public void testRemoteOnlyClassRemoved() {
        // Scenario 6: Remote-Only Class Removed → Expected Failure
        final Map<String, String> remoteSources = new HashMap<>();
        final AtomicBoolean providerActive = new AtomicBoolean(true);
        final AtomicBoolean providerDirty = new AtomicBoolean(true);

        String remoteOnly = "package test.notebook\n\n" +
                "return \"RemoteOnly\"\n\n" +
                "static String getType() {\n" +
                "    return \"REMOTE_ONLY\"\n" +
                "}\n\n" +
                "static int getCode() {\n" +
                "    return 555\n" +
                "}";

        remoteSources.put("test/notebook/RemoteOnly.groovy", remoteOnly);

        RemoteFileSourceProvider provider = createProvider(remoteSources, providerActive, providerDirty);
        RemoteFileSourceClassLoader cl = RemoteFileSourceClassLoader.getInstance();
        try {
            cl.registerProvider(provider);

            // Step 1: Use RemoteOnly class
            ScriptSession.Changes c1 = session.evaluateScript(
                    "import test.notebook.RemoteOnly\n" +
                            "ExecutionContext.getContext().getQueryLibrary().importClass(RemoteOnly.class)\n" +
                            "t1 = emptyTable(1).updateView(\"Type = RemoteOnly.getType()\", \"Code = RemoteOnly.getCode()\")");
            c1.throwIfError();

            Table t1 = session.getQueryScope().readParamValue("t1");
            assertEquals("REMOTE_ONLY", (String) t1.getColumnSource("Type").get(0));
            assertEquals(555, t1.getColumnSource("Code").getInt(0));

            // Step 2: Deactivate provider (makes RemoteOnly unavailable)
            providerActive.set(false);
            providerDirty.set(true);

            // Step 3: Try to use RemoteOnly again - should fail
            ScriptSession.Changes c2 = session.evaluateScript(
                    "import test.notebook.RemoteOnly\n" +
                            "ExecutionContext.getContext().getQueryLibrary().importClass(RemoteOnly.class)\n" +
                            "t2 = emptyTable(1).updateView(\"Type = RemoteOnly.getType()\", \"Code = RemoteOnly.getCode()\")");

            assertTrue("Script should fail when remote-only class is removed",
                    c2.error != null);
        } finally {
            cl.unregisterProvider(provider);
        }
    }
}
