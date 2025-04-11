//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;

import java.util.Set;

/**
 * Limits the allowed HTTP methods to those specified in the configuration. When set on the http configuration itself,
 * this is applied for all calls made to the server, regardless of context.
 * <p>
 * For the deephaven-core server, we need to allow only one HTTP method, with two other optional ones, depending on how
 * the server is to be used.
 * <ul>
 * <li>POST - required for the server to be useful at all, used for all gRPC endpoints that deephaven-core
 * supports.</li>
 * <li>GET - optional, likely only needed for browser clients at this time.</li>
 * <li>OPTIONS - optional, necessary for CORS requests.</li>
 * </ul>
 */
public class AllowedHttpMethodsCustomizer implements HttpConfiguration.Customizer {
    private final Set<String> allowedMethods;

    /**
     * Creates a new instance of the customizer, allowing only the specified HTTP methods.
     *
     * @param allowedMethods the HTTP methods to allow
     */
    public AllowedHttpMethodsCustomizer(final Set<String> allowedMethods) {
        this.allowedMethods = allowedMethods;
    }

    @Override
    public void customize(final Connector connector, final HttpConfiguration httpConfiguration, final Request request) {
        if (!allowedMethods.contains(request.getMethod())) {
            request.setHandled(true);
            request.getResponse().setStatus(HttpStatus.METHOD_NOT_ALLOWED_405);
        }
    }
}
