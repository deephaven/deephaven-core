//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;

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
    public void customize(final Connector connector, final HttpConfiguration channelConfig, final Request request) {
        final Response response = request.getResponse();
        for (final Map.Entry<String, String> header : configuredHeaders.entrySet()) {
            response.setHeader(header.getKey(), header.getValue());
        }
    }
}
