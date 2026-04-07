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

    private final Map<String, String> remoteSources = new HashMap<>();
    private final AtomicBoolean providerDirty = new AtomicBoolean(false);
    private RemoteFileSourceProvider provider;

    private String scriptServerDependency;
    private String scriptRemoteOnlyDependency;

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

        scriptServerDependency = loadTestScript("/test-scripts/remote-test-entrypoint.groovy");
        scriptRemoteOnlyDependency = loadTestScript("/test-scripts/remote-only-entrypoint.groovy");
    }

    @After
    public void teardown() {
        RemoteFileSourceClassLoader.getInstance().unregisterProvider(provider);
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

    private static final String PATH_ON_SERVER = "test/notebook/Helper.groovy";
    private static final String PATH_NOT_ON_SERVER = "test/notebook/RemoteOnly.groovy";
    private static final String REMOTE_SOURCE = remoteHelperSource("REMOTE", 999);

    private static String remoteHelperSource(String version, int value) {
        return "package test.notebook\n\n" +
                "return \"Helper\"\n\n" +
                "static String getVersion() {\n" +
                "    return \"" + version + "\"\n" +
                "}\n\n" +
                "static int getValue() {\n" +
                "    return " + value + "\n" +
                "}\n\n" +
                "class HelperClass {\n" +
                "    final String source = \"" + version + "\"\n" +
                "    String getSource() {\n" +
                "        return source\n" +
                "    }\n" +
                "}\n\n" +
                "static String getSourceViaClass() {\n" +
                "    new HelperClass().getSource()\n" +
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
                scriptServerDependency,
                Map.of(),
                false,
                "SERVER",
                100);
    }

    @Test
    public void testRemoteOverridesServer() {
        evaluateAndAssertHelper(
                "remote overrides server",
                scriptServerDependency,
                Map.of(PATH_ON_SERVER, REMOTE_SOURCE),
                true,
                "REMOTE",
                999);
    }

    @Test
    public void testServerToRemoteAndBack() {
        evaluateAndAssertHelper(
                "step 1: server",
                scriptServerDependency,
                Map.of(),
                false,
                "SERVER",
                100);

        evaluateAndAssertHelper(
                "step 2: server→remote",
                scriptServerDependency,
                Map.of(PATH_ON_SERVER, REMOTE_SOURCE),
                true,
                "REMOTE",
                999);

        evaluateAndAssertHelper(
                "step 3: remote→server",
                scriptServerDependency,
                Map.of(),
                true,
                "SERVER",
                100);
    }

    @Test
    public void testRemoteToServer() {
        evaluateAndAssertHelper(
                "step 1: remote",
                scriptServerDependency,
                Map.of(PATH_ON_SERVER, REMOTE_SOURCE),
                true,
                "REMOTE",
                999);

        evaluateAndAssertHelper(
                "step 2: remote→server",
                scriptServerDependency,
                Map.of(),
                true,
                "SERVER",
                100);
    }

    @Test
    public void testIsDirtyClearsCacheWithinRemote() {
        evaluateAndAssertHelper(
                "step 1: remote v1",
                scriptServerDependency,
                Map.of(PATH_ON_SERVER, REMOTE_SOURCE),
                true,
                "REMOTE",
                999);

        evaluateAndAssertHelper(
                "step 2: remote v2",
                scriptServerDependency,
                Map.of(PATH_ON_SERVER, remoteHelperSource("REMOTE_V2", 777)),
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
                scriptRemoteOnlyDependency,
                Map.of(PATH_NOT_ON_SERVER, remoteHelperSource("REMOTE_ONLY", 555)),
                true,
                "REMOTE_ONLY",
                555);

        // Step 2: Remove remote sources — no server fallback exists, should fail
        remoteSources.clear();
        providerDirty.set(true);

        ScriptSession.Changes c2 = session.evaluateScript(scriptRemoteOnlyDependency);
        assertTrue("Script should fail when remote-only class is removed", c2.error != null);
    }
}
