package io.deephaven.test.types;

/**
 * An out-of-band test is a test that is not typically run on a commit-by-commit basis; usually because it is expensive
 * and will delay the typical CI feedback cycle.
 */
@SuppressWarnings("JavadocReference")
public interface OutOfBandTest {
}
