//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CredentialsTest {

    @Test
    public void defaultCredentials() {
        isCredentials(Credentials.defaultCredentials());
    }

    @Test
    public void basic() {
        isCredentials(Credentials.basic("accessKeyId", "secretAccessKey"));
    }

    @Test
    public void anonymous() {
        isCredentials(Credentials.anonymous());
    }

    private static void isCredentials(final Credentials c) {
        assertThat(c).isInstanceOf(AwsSdkV2Credentials.class);
    }
}
