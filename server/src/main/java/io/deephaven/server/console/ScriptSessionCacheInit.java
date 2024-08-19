//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console;

import io.deephaven.engine.util.AbstractScriptSession;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ScriptSessionCacheInit {

    @Inject
    public ScriptSessionCacheInit() {
        // create the script cache (or clear previous sessions)
        AbstractScriptSession.createScriptCache();
    }
}
