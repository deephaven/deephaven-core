package io.deephaven.server.plugin.java;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.PluginCallback;

import java.util.ServiceLoader;

public final class JavaServiceLoader {
    public static void allRegisterInto(PluginCallback callback) {
        for (Plugin provider : ServiceLoader.load(Plugin.class)) {
            provider.registerInto(callback);
        }
    }
}
