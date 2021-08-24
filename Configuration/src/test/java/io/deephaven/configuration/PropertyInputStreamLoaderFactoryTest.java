package io.deephaven.configuration;

import junit.framework.TestCase;

public class PropertyInputStreamLoaderFactoryTest extends TestCase {

    /**
     * Check that {@link PropertyInputStreamLoaderFactory#newInstance()} is an instance of
     * {@link PropertyInputStreamLoaderTraditional}.
     *
     * <p>
     * The fishlib tests always assume the "traditional" method of loading prop files. If some day
     * this is not the case, then this test will need to be updated to reflect new fishlib testing
     * assumptions wrt {@link PropertyInputStreamLoaderFactory}.
     */
    public void testInstanceIsTraditional() {
        assertTrue(PropertyInputStreamLoaderFactory
            .newInstance() instanceof PropertyInputStreamLoaderTraditional);
    }
}
