package org.jpy;

import org.junit.Test;

/**
 * Wraps up {@link EmbeddableTest} with JUnit so {@link EmbeddableTest} doesn't have to have the
 * JUnit dependency.
 */
public class EmbeddableTestJunit {
    @Test
    public void testStartingAndStoppingIfAvailable() {
        EmbeddableTest.testStartingAndStoppingIfAvailable();
    }

    @Test
    public void testPassStatement() {
        EmbeddableTest.testPassStatement();
    }

    @Test
    public void testPrintStatement() {
        EmbeddableTest.testPrintStatement();
    }

    @Test
    public void testIncrementByOne() {
        EmbeddableTest.testIncrementByOne();
    }
}
