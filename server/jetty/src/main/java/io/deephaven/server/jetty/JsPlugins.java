package io.deephaven.server.jetty;

import io.deephaven.configuration.ConfigDir;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.util.resource.PathResource;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Consumer;

import static org.eclipse.jetty.servlet.ServletContextHandler.NO_SESSIONS;

class JsPlugins {

    private static final Logger log = LoggerFactory.getLogger(JsPlugins.class);

    public static void maybeAdd(Consumer<Handler> addHandler) {
        resource()
                .map(ControlledCacheResource::wrap)
                .ifPresent(resource -> addResource(addHandler, resource));
    }

    private static void addResource(Consumer<Handler> addHandler, ControlledCacheResource resource) {
        log.info().append("Creating JsPlugins context with resource '").append(resource.toString()).append('\'').endl();
        WebAppContext context =
                new WebAppContext(null, "/js-plugins/", null, null, null, new ErrorPageErrorHandler(), NO_SESSIONS);
        context.setBaseResource(resource);
        context.setInitParameter(DefaultServlet.CONTEXT_INIT + "dirAllowed", "false");
        // Suppress warnings about security handlers
        context.setSecurityHandler(new ConstraintSecurityHandler());
        addHandler.accept(context);
    }

    private static Optional<Resource> resource() {
        // Note: this would probably be better to live in JettyConfig - but until we establish more formal expectations
        // for js plugin configuration and workflows, we'll keep this here.
        final String resourceBase =
                Configuration.getInstance().getStringWithDefault("deephaven.jsPlugins.resourceBase", null);
        if (resourceBase == null) {
            // defaults to "<configDir>/js-plugins/" if it exists
            return defaultJsPluginsDirectory()
                    .filter(Files::exists)
                    .map(PathResource::new);
        }
        try {
            return Optional.of(Resource.newResource(resourceBase));
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Unable to resolve resourceBase '%s'", resourceBase), e);
        }
    }

    private static Optional<Path> defaultJsPluginsDirectory() {
        return ConfigDir.get().map(JsPlugins::jsPluginsDir);
    }

    private static Path jsPluginsDir(Path configDir) {
        return configDir.resolve("js-plugins");
    }
}
