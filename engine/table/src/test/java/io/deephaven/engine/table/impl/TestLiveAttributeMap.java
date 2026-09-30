//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link LiveAttributeMap}.
 */
@RunWith(Parameterized.class)
public class TestLiveAttributeMap {

    @Parameterized.Parameters(name = "copyViaSetAttribute={0}")
    public static Collection<Object[]> parameters() {
        return List.of(new Object[] {false}, new Object[] {true});
    }

    private static final class AttrMap extends LiveAttributeMap<AttrMap, AttrMap> {

        /**
         * Whether {@link #copy()} fills the result with {@link #copyAttributes}, as the table implementations do,
         * rather than passing our attributes as the initial attributes, as the hierarchical table implementations do.
         */
        private final boolean copyViaSetAttribute;

        private AttrMap(@Nullable final Map<String, Object> initialAttributes, final boolean copyViaSetAttribute) {
            super(initialAttributes, false);
            this.copyViaSetAttribute = copyViaSetAttribute;
        }

        @Override
        protected AttrMap copy() {
            if (!copyViaSetAttribute) {
                return new AttrMap(getAttributes(), false);
            }
            final AttrMap result = new AttrMap(null, true);
            copyAttributes(this, result, ak -> true);
            return result;
        }
    }

    private final boolean copyViaSetAttribute;
    private final List<LivenessScope> scopes = new ArrayList<>();

    public TestLiveAttributeMap(final boolean copyViaSetAttribute) {
        this.copyViaSetAttribute = copyViaSetAttribute;
    }

    @After
    public void tearDown() {
        scopes.forEach(LivenessScope::release);
    }

    private void release(@NotNull final LivenessScope scope) {
        assertTrue(scopes.remove(scope));
        scope.release();
    }

    private LivenessScope newScope() {
        final LivenessScope scope = new LivenessScope();
        scopes.add(scope);
        return scope;
    }

    private static <T> T inScope(@NotNull final LivenessScope scope, @NotNull final Supplier<T> supplier) {
        try (final SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            return supplier.get();
        }
    }

    private AttrMap newMap(@NotNull final LivenessScope scope, @Nullable final Map<String, Object> initialAttributes) {
        return inScope(scope, () -> new AttrMap(initialAttributes, copyViaSetAttribute));
    }

    /**
     * Set a new {@link LivenessArtifact} as the value for {@code key} in {@code map}, leaving {@code map} as its only
     * manager.
     */
    private static LivenessArtifact setReferent(@NotNull final AttrMap map, @NotNull final String key) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final LivenessArtifact value = new LivenessArtifact() {};
            map.setAttribute(key, value);
            return value;
        }
    }

    private static boolean isLive(@NotNull final LivenessReferent referent) {
        if (referent.tryRetainReference()) {
            referent.dropReference();
            return true;
        }
        return false;
    }

    @Test
    public void testEmpty() {
        final AttrMap empty = new AttrMap(null, copyViaSetAttribute);
        final Map<String, Object> emptyAttrs = empty.getAttributes();
        assertTrue(emptyAttrs.isEmpty());
    }

    @Test
    public void testSetAttributeReplacementUnmanages() {
        final AttrMap map = newMap(newScope(), null);
        final LivenessArtifact value = setReferent(map, "k");
        assertTrue(isLive(value));
        map.setAttribute("k", "replacement");
        assertFalse(isLive(value));
    }

    @Test
    public void testSetAttributeSameValueManagesOnce() {
        final AttrMap map = newMap(newScope(), null);
        final LivenessArtifact value = setReferent(map, "k");
        map.setAttribute("k", value);
        map.setAttribute("k", (Object existing) -> value);
        map.setAttribute("k", "replacement");
        assertFalse(isLive(value));
    }

    @Test
    public void testSetAttributeUpdaterReplacementUnmanages() {
        final AttrMap map = newMap(newScope(), null);
        final LivenessArtifact value = setReferent(map, "k");
        map.setAttribute("k", (Object existing) -> "replacement");
        assertFalse(isLive(value));
    }

    @Test
    public void testInitialAttributesManaged() {
        final LivenessScope mapScope = newScope();
        final LivenessArtifact value;
        final AttrMap map;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            value = new LivenessArtifact() {};
            map = newMap(mapScope, Map.of("k", value));
        }
        assertTrue(isLive(value));
        assertSame(value, map.getAttribute("k"));
        release(mapScope);
        assertFalse(isLive(value));
    }

    @Test
    public void testCopyManagesRetainedValuesOnce() {
        final LivenessScope originalScope = newScope();
        final AttrMap original = newMap(originalScope, null);
        final LivenessArtifact value = setReferent(original, "k");

        final AttrMap copy = inScope(newScope(), () -> original.withAttributes(Map.of("other", "o")));
        assertNotSame(original, copy);
        release(originalScope);
        assertTrue(isLive(value));
        copy.setAttribute("k", "replacement");
        assertFalse(isLive(value));
    }

    @Test
    public void testWithAttributesReplacementReleases() {
        checkCopyReleases(original -> original.withAttributes(Map.of("k", "replacement")));
    }

    @Test
    public void testWithAttributesRemovalReleases() {
        checkCopyReleases(original -> original.withAttributes(Map.of("other", "o"), List.of("k")));
    }

    @Test
    public void testWithAttributesAddAndRemoveReplacementReleases() {
        checkCopyReleases(original -> original.withAttributes(Map.of("k", "replacement"), List.of("k")));
    }

    @Test
    public void testWithoutAttributesReleases() {
        checkCopyReleases(original -> original.withoutAttributes(List.of("k")));
    }

    @Test
    public void testRetainingAttributesReleases() {
        checkCopyReleases(original -> original.retainingAttributes(List.of("other")));
    }

    /**
     * Verify that a copy made by {@code operation} that drops or replaces the referent at {@code "k"} does not keep it
     * live after the original is released.
     */
    private void checkCopyReleases(@NotNull final UnaryOperator<AttrMap> operation) {
        final LivenessScope originalScope = newScope();
        final AttrMap original = newMap(originalScope, null);
        final LivenessArtifact value = setReferent(original, "k");
        original.setAttribute("other", "o");

        final AttrMap copy = inScope(newScope(), () -> operation.apply(original));
        assertNotSame(original, copy);
        assertNotSame(value, copy.getAttribute("k"));
        assertTrue(isLive(value));
        release(originalScope);
        assertFalse(isLive(value));
    }
}
