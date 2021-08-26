package io.deephaven.extensions;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.libs.QueryLibraryImports;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.GroovyDeephavenSession.InitScript;
import io.deephaven.util.QueryConstants;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.Resource;
import io.github.classgraph.ScanResult;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;

public class ClassGraphExtension {

    public static class Script implements InitScript {

        @Inject
        public Script() {}

        @Override
        public String getScriptPath() {
            return "classgraph-init.groovy";
        }

        @Override
        public int priority() {
            return 100;
        }
    }

    public static class Imports implements QueryLibraryImports {

        @Override
        public Set<Package> packages() {
            return Collections.emptySet();
        }

        @Override
        public Set<Class<?>> classes() {
            return Collections.emptySet();
        }

        @Override
        public Set<Class<?>> statics() {
            return Collections.emptySet();
        }
    }

    public static void printAllExtensions(String extension, PrintStream out) {
        try (ScanResult scanResult = new ClassGraph().scan()) {
            for (Resource resource : scanResult.getResourcesWithExtension(extension)) {
                out.println(resource);
            }
        }
    }

    public static Table tableAllExtensions(String extension, ScanResult scan) {
        return fromUris(scan
            .getResourcesWithExtension(extension)
            .stream()
            .map(Resource::getURI)
            .iterator());
    }

    public static Table tableAllExtensions(String extension) {
        try (final ScanResult scan = new ClassGraph().scan()) {
            return tableAllExtensions(extension, scan);
        }
    }

    public static Table fromUris(Iterator<URI> it) {
        List<String> schemes = new ArrayList<>();
        List<String> schemeSpecificParts = new ArrayList<>();
        List<String> authorities = new ArrayList<>();
        List<String> userInfos = new ArrayList<>();
        List<String> hosts = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        List<String> paths = new ArrayList<>();
        List<String> queries = new ArrayList<>();
        List<String> fragments = new ArrayList<>();
        while (it.hasNext()) {
            URI uri = it.next();
            schemes.add(uri.getScheme());
            schemeSpecificParts.add(uri.getSchemeSpecificPart());
            authorities.add(uri.getAuthority());
            userInfos.add(uri.getUserInfo());
            hosts.add(uri.getHost());
            int port = uri.getPort(); // -1 if undefined
            ports.add(port == -1 ? null : port);
            paths.add(uri.getPath());
            queries.add(uri.getQuery());
            fragments.add(uri.getFragment());
        }
        return TableTools.newTable(
            TableTools.stringCol("Scheme", schemes.toArray(new String[0])),
            TableTools.stringCol("SchemeSpecificPart", schemeSpecificParts.toArray(new String[0])),
            TableTools.stringCol("Authority", authorities.toArray(new String[0])),
            TableTools.stringCol("UserInfo", userInfos.toArray(new String[0])),
            TableTools.stringCol("Host", hosts.toArray(new String[0])),
            TableTools.intCol("Port",
                ports.stream().mapToInt(ClassGraphExtension::toDhInt).toArray()),
            TableTools.stringCol("Path", paths.toArray(new String[0])),
            TableTools.stringCol("Query", queries.toArray(new String[0])),
            TableTools.stringCol("Fragment", fragments.toArray(new String[0])));
    }

    private static int toDhInt(Integer boxed) {
        return boxed == null ? QueryConstants.NULL_INT : boxed;
    }
}
