//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import dagger.Module;
import dagger.Provides;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.util.ScriptSession;

import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Map;

/**
 * Provides a {@link ScriptSession}.
 */
@Module
public class ScriptSessionModule {

    @Provides
    @Singleton
    public static ScriptSession provideScriptSession(Map<String, Provider<ScriptSession>> scriptTypes) {
        // Check which script language is configured
        String scriptSessionType = Configuration.getInstance().getProperty("deephaven.console.type");

        // Emit an error if the selected language isn't provided
        if (!scriptTypes.containsKey(scriptSessionType)) {
            throw new IllegalArgumentException("Console type not found: " + scriptSessionType);
        }
        return scriptTypes.get(scriptSessionType).get();
    }
}
