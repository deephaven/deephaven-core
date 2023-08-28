package io.deephaven.server.jetty;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.configuration.ConfigDir;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

class JsPlugins {
    private static final String RESOURCE_BASE = "resourceBase";
    private static final String DEEPHAVEN_JS_PLUGINS_PREFIX = "deephaven.jsPlugins.";
    static final String MANIFEST_JSON = "manifest.json";
    private static final String PACKAGE_JSON = "package.json";
    static final String JS_PLUGINS = "js-plugins";

    private static final Logger log = LoggerFactory.getLogger(JettyBackedGrpcServer.class);

    static JsPluginsZipFilesystem initJsPlugins(Configuration config) throws IOException {
        final JsPluginsZipFilesystem fs = JsPluginsZipFilesystem.create();
        // deephaven.jsPlugins.resourceBase
        {
            final String resourceBase = config.getStringWithDefault(DEEPHAVEN_JS_PLUGINS_PREFIX + RESOURCE_BASE, null);
            if (resourceBase != null) {
                addAllManifestBase(fs, Path.of(resourceBase));
            }
        }
        // <configDir>/js-plugins/
        {
            final Path resourceBase = ConfigDir.get()
                    .map(p -> p.resolve(JS_PLUGINS).resolve(MANIFEST_JSON))
                    .filter(Files::exists)
                    .map(Path::getParent)
                    .orElse(null);
            if (resourceBase != null) {
                addAllManifestBase(fs, resourceBase);
            }
        }
        // deephaven.jsPlugins.<part>
        {
            final Set<String> parts = partsThatStartWith(DEEPHAVEN_JS_PLUGINS_PREFIX, config);
            for (String part : parts) {
                if (RESOURCE_BASE.equals(part)) {
                    // handled earlier
                    continue;
                }
                final String packageBase = config.getStringWithDefault(DEEPHAVEN_JS_PLUGINS_PREFIX + part, null);
                if (packageBase != null) {
                    final Path packageBasePath = Path.of(packageBase);
                    final Path packageJson = packageBasePath.resolve(PACKAGE_JSON);
                    final JsPlugin plugin = plugin(packageJson);
                    log.info().append("Adding JsPlugin ").append(plugin.name()).append(" from ")
                            .append(packageJson.toString()).endl();
                    fs.addFromPackageBase(packageBasePath, plugin);
                }
            }
        }
        return fs;
    }

    private static void addAllManifestBase(JsPluginsZipFilesystem fs, Path srcManifestBase) throws IOException {
        final Path manifestJson = srcManifestBase.resolve(MANIFEST_JSON);
        final JsManifest manifestInfo = manifest(manifestJson);
        for (JsPlugin plugin : manifestInfo.plugins()) {
            log.info().append("Adding JsPlugin ").append(plugin.name()).append(" from ").append(manifestJson.toString())
                    .endl();
            fs.addFromManifestBase(srcManifestBase, plugin);
        }
    }

    private static JsManifest manifest(Path manifestJson) throws IOException {
        final ObjectMapper om = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);;
        try (final InputStream in = Files.newInputStream(manifestJson)) {
            return om.readValue(in, JsManifest.class);
        }
    }

    private static JsPlugin plugin(Path packageJson) throws IOException {
        final ObjectMapper om = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try (final InputStream in = Files.newInputStream(packageJson)) {
            return om.readValue(in, JsPlugin.class);
        }
    }

    private static Set<String> partsThatStartWith(String prefix, Configuration configuration) {
        final Set<String> parts = new HashSet<>();
        final Iterator<Object> it = configuration.getProperties(prefix).keys().asIterator();
        while (it.hasNext()) {
            final Object next = it.next();
            if (next instanceof String) {
                parts.add(firstPart((String) next));
            }
        }
        return parts;
    }

    private static String firstPart(String x) {
        final int index = x.indexOf('.');
        return index == -1 ? x : x.substring(0, index);
    }
}
