//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.assertj.core.api.Assumptions.assumeThat;

class MissingHadoopShadedGuavaTest {

    // We can't test both getProperty and setProperty in the same JVM given the stateful nature classloading errors.
    // We _could_ create a separate class and use `forkEvery = 1`, but that is extra test orchestration that doesn't add
    // much value. We'll choose to randomly test one of them for any given test run. (This could theoretically cause
    // issues if something downstream alerted about tests that flapped from "SUCCESS" to "IGNORED", but we don't have
    // anything like that right now.)
    private static final boolean TEST_GET_PROPERTY = ThreadLocalRandom.current().nextBoolean();
    private static final boolean TEST_SET_PROPERTY = !TEST_GET_PROPERTY;

    @Test
    void constructConfiguration() {
        new Configuration();
    }

    /**
     * If this test is able to successfully get a property, it likely means that hadoop Configuration no longer depends
     * on hadoop-shaded-guava, and we should likely remove it from hadoop-common-dependencies.
     */
    @Test
    void getProperty() {
        assumeThat(TEST_GET_PROPERTY).isTrue();
        final Configuration configuration = new Configuration();
        try {
            configuration.get("some.property");
            failBecauseExceptionWasNotThrown(NoClassDefFoundError.class);
        } catch (final NoClassDefFoundError e) {
            assertThat(e).hasMessage("org/apache/hadoop/thirdparty/com/google/common/collect/Interners");
            assertThat(e).hasCauseInstanceOf(ClassNotFoundException.class);
        }
    }

    /**
     * If this test is able to successfully set a property, it likely means that hadoop Configuration no longer depends
     * on hadoop-shaded-guava, and we should likely remove it from hadoop-common-dependencies.
     */
    @Test
    void setProperty() {
        assumeThat(TEST_SET_PROPERTY).isTrue();
        final Configuration configuration = new Configuration();
        try {
            configuration.set("some.property", "some.value");
            failBecauseExceptionWasNotThrown(NoClassDefFoundError.class);
        } catch (final NoClassDefFoundError e) {
            assertThat(e).hasMessage("org/apache/hadoop/thirdparty/com/google/common/collect/Interners");
            assertThat(e).hasCauseInstanceOf(ClassNotFoundException.class);
        }
    }
}
