package io.deephaven.server.jetty;

import io.deephaven.configuration.Configuration;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Consumer;

import static org.eclipse.jetty.servlet.ServletContextHandler.NO_SESSIONS;

class JsPlugins {

    public static void maybeAdd(Consumer<Handler> addHandler) {
        // Note: this would probably be better to live in JettyConfig - but until we establish more formal expectations
        // for js plugin configuration and workflows, we'll keep this here.
        final String resourceBase =
                Configuration.getInstance().getStringWithDefault("deephaven.jsPlugins.resourceBase", null);
        if (resourceBase == null) {
            return;
        }
        try {
            Resource resource = ControlledCacheResource.wrap(Resource.newResource(resourceBase));
            WebAppContext context =
                    new WebAppContext(null, "/js-plugins/", null, null, null, new ErrorPageErrorHandler(), NO_SESSIONS);
            context.setBaseResource(resource);
            context.setInitParameter(DefaultServlet.CONTEXT_INIT + "dirAllowed", "false");
            // Suppress warnings about security handlers
            context.setSecurityHandler(new ConstraintSecurityHandler());
            addHandler.accept(context);
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Unable to resolve resourceBase '%s'", resourceBase), e);
        }
    }
}
