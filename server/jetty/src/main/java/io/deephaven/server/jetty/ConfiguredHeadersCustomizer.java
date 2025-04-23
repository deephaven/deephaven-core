//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;

import java.util.Map;

/**
 * Applies configured http headers to every outgoing response.
 */
public class ConfiguredHeadersCustomizer implements HttpConfiguration.Customizer {
    private final Map<String, String> configuredHeaders;

    /**
     * Creates a new instance of the customizer, applying the given headers to every outgoing response.
     *
     * @param configuredHeaders the headers to add to every response
     */
    public ConfiguredHeadersCustomizer(final Map<String, String> configuredHeaders) {
        this.configuredHeaders = configuredHeaders;
    }

    @Override
    public Request customize(final Request request, final HttpFields.Mutable responseHeaders) {
        for (Map.Entry<String, String> header : configuredHeaders.entrySet()) {
            responseHeaders.add(header.getKey(), header.getValue());
        }
        return request;
    }
}
