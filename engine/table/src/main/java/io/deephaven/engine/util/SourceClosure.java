package io.deephaven.engine.util;

import io.deephaven.engine.util.scripts.ScriptPathLoader;
import groovy.lang.Closure;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

/**
 * This class represents uses of the source() and sourceOnce() method calls. It will use the underlying scriptPathLoader
 * to get the script text to use.
 */
public class SourceClosure extends Closure<Object> {
    private final ScriptPathLoader scriptPathLoader;
    private final boolean sourceOnce;
    private final boolean caching;

    // Attempt to cache scripts as long as memory permits.
    private final Map<String, SoftReference<String>> scriptCache;

    public SourceClosure(final GroovyDeephavenSession groovySession, final ScriptPathLoader scriptPathLoader,
            final boolean sourceOnce, final boolean caching) {
        super(groovySession, null);
        this.scriptPathLoader = scriptPathLoader;
        this.sourceOnce = sourceOnce;
        this.caching = caching;

        scriptCache = caching ? new HashMap<>() : null;
    }

    @Override
    public String call(Object... args) {
        final GroovyDeephavenSession groovyDeephavenSession = (GroovyDeephavenSession) getOwner();
        final String scriptName = (String) args[0];

        if (sourceOnce) {
            if (groovyDeephavenSession.hasExecutedScript(scriptName)) {
                return null;
            }
        }

        // If we're caching, try to grab the cached value first
        String scriptText = null;
        if (caching) {
            final SoftReference<String> cacheRef = scriptCache.get(scriptName);
            if (cacheRef != null) {
                scriptText = cacheRef.get();

                if (scriptText == null) {
                    scriptCache.remove(scriptName);
                }
            }
        }

        // If we were unsuccessful, go to the controller
        if (scriptText == null) {
            try {
                scriptText = scriptPathLoader.getScriptBodyByRelativePath(scriptName);
            } catch (IOException e) {
                throw new UncheckedIOException("Could not load \"" + scriptName, e);
            }

            if (caching) {
                scriptCache.put(scriptName, new SoftReference<>(scriptText));
            }
        }

        groovyDeephavenSession.evaluateScript(scriptText, scriptName);
        return null;
    }

    public void clearCache() {
        if (caching) {
            scriptCache.clear();
        }
    }

    public ScriptPathLoader getPathLoader() {
        return scriptPathLoader;
    }
}
