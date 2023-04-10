/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

/**
 * A container for configuration values from the server.
 */
public interface ServerConfigValues {
    String GRADLE_VERSION = "gradleVersion";
    String VCS_VERSION = "vcsVersion";
    String JAVA_VERSION = "javaVersion";

    String WORKER_KINDS = "workerKinds";
}
