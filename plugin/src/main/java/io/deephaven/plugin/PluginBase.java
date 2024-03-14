//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin;

public abstract class PluginBase implements Plugin {

    @Override
    public final void registerInto(Callback callback) {
        callback.register(this);
    }
}
