//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.grpc;

import io.grpc.Grpc;

import java.util.List;

public final class UserAgentUtility {

    /**
     * Constructs a <a href="https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#user-agents">grpc
     * user-agent</a> of the form {@code grpc-java/<grpc-java-version>} or
     * {@code grpc-java/<grpc-java-version> (prop1; ...; propN)}.
     *
     * @param properties the properties
     * @return the user-agent
     * @see <a href="https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#user-agents">grpc user-agents</a>
     * @see #versionProperty(String, String)
     * @see #versionProperty(String, Class)
     */
    public static String userAgent(List<String> properties) {
        final String grpcJavaVersionProperty = versionProperty("grpc-java", Grpc.class);
        return properties.isEmpty()
                ? grpcJavaVersionProperty
                : String.format("%s (%s)", grpcJavaVersionProperty, String.join("; ", properties));
    }

    /**
     * Constructs a version property in the
     * <a href="https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#user-agents">grpc user-agent</a> style.
     *
     * @param name the name
     * @param version the version
     * @return the version property
     */
    public static String versionProperty(String name, String version) {
        return name + "/" + version;
    }

    /**
     * Constructs a version property in the
     * <a href="https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#user-agents">grpc user-agent</a> style.
     * Uses {@code clazz.getPackage().getImplementationVersion()} as the version string.
     *
     * @param name the name
     * @param clazz the class to take the version from
     * @return the version property
     */
    public static String versionProperty(String name, Class<?> clazz) {
        return versionProperty(name, clazz.getPackage().getImplementationVersion());
    }
}
