//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

class MissingWoodstoxTest {

    /**
     * If this test is able to successfully create a Configuration, it likely means that hadoop Configuration no longer
     * depends on woodstox, and we should likely remove it from hadoop-common-dependencies.
     */
    @Test
    void constructConfiguration() {
        try {
            new Configuration();
            failBecauseExceptionWasNotThrown(NoClassDefFoundError.class);
        } catch (final NoClassDefFoundError e) {
            assertThat(e).hasMessage("com/ctc/wstx/io/InputBootstrapper");
            assertThat(e).hasCauseInstanceOf(ClassNotFoundException.class);
        }
    }
}
