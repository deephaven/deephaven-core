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

    public ConfiguredHeadersCustomizer(Map<String, String> configuredHeaders) {
        this.configuredHeaders = configuredHeaders;
    }

    @Override
    public void customize(Connector connector, HttpConfiguration channelConfig, Request request) {
        Response response = request.getResponse();
        for (Map.Entry<String, String> header : configuredHeaders.entrySet()) {
            response.setHeader(header.getKey(), header.getValue());
        }
    }
}
