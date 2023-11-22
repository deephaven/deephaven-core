package io.deephaven.engine.table.impl;

import junit.framework.TestCase;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.util.Map;

/**
 * Unit tests for {@link LiveAttributeMap}.
 */
public class TestLiveAttributeMap {

    private static final class AttrMap extends LiveAttributeMap<AttrMap, AttrMap> {

        private AttrMap(@Nullable final Map<String, Object> initialAttributes) {
            super(initialAttributes);
        }

        @Override
        protected AttrMap copy() {
            return new AttrMap(getAttributes());
        }

    }

    @Test
    public void testEmpty() {
        final AttrMap empty = new AttrMap(null);
        final Map<String, Object> emptyAttrs = empty.getAttributes();
        TestCase.assertTrue(emptyAttrs.isEmpty());
    }
}
