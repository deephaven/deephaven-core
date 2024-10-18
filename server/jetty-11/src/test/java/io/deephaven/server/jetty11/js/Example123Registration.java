//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11.js;

import io.deephaven.plugin.Registration;
import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.js.Paths;

import java.net.URISyntaxException;
import java.nio.file.Path;

public final class Example123Registration implements Registration {

    public Example123Registration() {}

    @Override
    public void registerInto(Callback callback) {
        final JsPlugin example1;
        final JsPlugin example2;
        final JsPlugin example3;
        try {
            example1 = example1();
            example2 = example2();
            example3 = example3();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        callback.register(example1);
        callback.register(example2);
        callback.register(example3);
    }

    private static JsPlugin example1() throws URISyntaxException {
        final Path resourcePath = Path.of(Sentinel.class.getResource("examples/@deephaven_test/example1").toURI());
        final Path main = resourcePath.relativize(resourcePath.resolve("dist/index.js"));
        return JsPlugin.builder()
                .name("@deephaven_test/example1")
                .version("0.1.0")
                .main(main)
                .path(resourcePath)
                .build();
    }

    private static JsPlugin example2() throws URISyntaxException {
        final Path resourcePath = Path.of(Sentinel.class.getResource("examples/@deephaven_test/example2").toURI());
        final Path dist = resourcePath.relativize(resourcePath.resolve("dist"));
        final Path main = dist.resolve("index.js");
        return JsPlugin.builder()
                .name("@deephaven_test/example2")
                .version("0.2.0")
                .main(main)
                .path(resourcePath)
                .paths(Paths.ofPrefixes(dist))
                .build();
    }

    private static JsPlugin example3() throws URISyntaxException {
        final Path resourcePath = Path.of(Sentinel.class.getResource("examples/@deephaven_test/example3").toURI());
        final Path main = resourcePath.relativize(resourcePath.resolve("index.js"));
        return JsPlugin.builder()
                .name("@deephaven_test/example3")
                .version("0.3.0")
                .main(main)
                .path(resourcePath)
                .build();
    }
}
