package io.deephaven.server.jetty;

import io.deephaven.configuration.Configuration;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

class JsPlugins {

    public static void maybeAdd(ServletContextHandler context) {
        // Note: this would probably be better to live in JettyConfig - but until we establish more formal expectations
        // for js plugin configuration and workflows, we'll keep this here.
        final String resourceBase =
                Configuration.getInstance().getStringWithDefault("deephaven.jsPlugins.resourceBase", null);
        if (resourceBase == null) {
            return;
        }
        context.addServlet(createServlet("js-plugins", resourceBase), "/js-plugins/*");
    }

    private static ServletHolder createServlet(String name, String resourceBase) {
        final ServletHolder jsPlugins = new ServletHolder(name, DefaultServlet.class);
        jsPlugins.setInitParameter("resourceBase", resourceBase);
        jsPlugins.setInitParameter("pathInfoOnly", "true");
        jsPlugins.setInitParameter("dirAllowed", "false");
        jsPlugins.setAsyncSupported(true);
        return jsPlugins;
    }
}
