//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HadoopCommonDependenciesTest {
    @Test
    void constructConfiguration() {
        new Configuration();
    }

    @Test
    void getProperty() {
        final Configuration configuration = new Configuration();
        assertThat(configuration.get("some.property")).isNull();
    }

    @Test
    void setProperty() {
        final Configuration configuration = new Configuration();
        configuration.set("some.property", "some.value");
        assertThat(configuration.get("some.property")).isEqualTo("some.value");
    }
}
