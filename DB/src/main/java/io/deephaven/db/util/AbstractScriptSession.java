package io.deephaven.db.util;

import com.github.f4b6a3.uuid.UuidCreator;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.FileUtils;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.libs.QueryLibrary;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.util.liveness.LivenessScope;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.lang.parse.api.CompletionParseService;
import io.deephaven.lang.parse.api.CompletionParseServiceNoOp;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.*;

/**
 * This class exists to make all script sessions to be liveness artifacts, and provide a default implementation
 * for evaluateScript which handles liveness and diffs in a consistent way.
 */
public abstract class AbstractScriptSession extends LivenessScope implements ScriptSession, VariableProvider {
    public static final String CLASS_CACHE_LOCATION = Configuration.getInstance().getStringWithDefault("ScriptSession.classCacheDirectory", "/tmp/dh_class_cache");

    public static void createScriptCache() {
        final File classCacheDirectory = new File(CLASS_CACHE_LOCATION);
        createOrClearDirectory(classCacheDirectory);
    }

    private static void createOrClearDirectory(final File directory) {
        if (directory.exists()) {
            FileUtils.deleteRecursively(directory);
        }
        if (!directory.mkdirs()) {
            throw new UncheckedDeephavenException("Failed to create class cache directory " + directory.getAbsolutePath());
        }
    }

    private final File classCacheDirectory;

    protected final QueryScope queryScope;
    protected final QueryLibrary queryLibrary;
    protected final CompilerTools.Context compilerContext;
    private final CompletionParseService<?, ?, ?> parser;

    protected AbstractScriptSession(boolean isDefaultScriptSession) {
        final UUID scriptCacheId = UuidCreator.getRandomBased();
        classCacheDirectory = new File(CLASS_CACHE_LOCATION, UuidCreator.toString(scriptCacheId));
        createOrClearDirectory(classCacheDirectory);

        queryScope = newQueryScope();
        queryLibrary = QueryLibrary.makeNewLibrary();

        compilerContext = new CompilerTools.Context(classCacheDirectory, getClass().getClassLoader()) {
            {
                addClassSource(getFakeClassDestination());
            }

            @Override public File getFakeClassDestination() {
                return classCacheDirectory;
            }

            @Override public String getClassPath() {
                return classCacheDirectory.getAbsolutePath() + File.pathSeparatorChar + super.getClassPath();
            }
        };

        if (isDefaultScriptSession) {
            CompilerTools.setDefaultContext(compilerContext);
            QueryScope.setDefaultScope(queryScope);
            QueryLibrary.setDefaultLibrary(queryLibrary);
        }
        @SuppressWarnings("rawtypes")
        ServiceLoader<CompletionParseService> loader = ServiceLoader.load(CompletionParseService.class);
        @SuppressWarnings("rawtypes")
        final Iterator<CompletionParseService> itr = loader.iterator();
        if (itr.hasNext()) {
            parser = itr.next();
        } else {
            // hm, should maybe log to the user that we are using a do-nothing completer...
            parser = new CompletionParseServiceNoOp();
        }
    }

    @Override
    public final Changes evaluateScript(final String script, final @Nullable String scriptName) {
        final Map<String, Object> existingScope = new HashMap<>(getVariables());

        // store pointers to exist query scope static variables
        final QueryLibrary prevQueryLibrary = QueryLibrary.getLibrary();
        final CompilerTools.Context prevCompilerContext = CompilerTools.getContext();
        final QueryScope prevQueryScope = QueryScope.getScope();

        // retain any objects which are created in the executed code, we'll release them when the script session closes
        try (final SafeCloseable ignored = LivenessScopeStack.open(this, false)) {
            // point query scope static state to our session's state
            QueryScope.setScope(queryScope);
            CompilerTools.setContext(compilerContext);
            QueryLibrary.setLibrary(queryLibrary);

            // actually evaluate the script
            evaluate(script, scriptName);
        } finally {
            // restore pointers to query scope static variables
            QueryScope.setScope(prevQueryScope);
            CompilerTools.setContext(prevCompilerContext);
            QueryLibrary.setLibrary(prevQueryLibrary);
        }

        final Map<String, Object> newScope = new HashMap<>(getVariables());

        // produce a diff
        final Changes diff = new Changes();
        final Map<String, ExportedObjectType> types = new HashMap<>();
        for (final Map.Entry<String, Object> entry : newScope.entrySet()) {
            final Object value = entry.getValue();
            final ExportedObjectType type = ExportedObjectType.fromObject(value);
            if (!type.isDisplayableInSwing()) {
                continue;
            }
            types.put(entry.getKey(), type);
            final Object existingObject = existingScope.get(entry.getKey());
            // if the name still has a binding but the type changed, it is a removed+created
            if (ExportedObjectType.fromObject(existingObject) == type) {
                if (unwrapObject(value) != unwrapObject(existingObject)) {
                    diff.updated.put(entry.getKey(), type);
                }
            } else {
                diff.created.put(entry.getKey(), type);
            }
        }

        for (final Map.Entry<String, Object> entry : existingScope.entrySet()) {
            final String name = entry.getKey();
            final Object value = entry.getValue();

            final ExportedObjectType type = ExportedObjectType.fromObject(value);
            if (type.isDisplayableInSwing()) {
                if (type != types.get(name)) {
                    // either the name no longer exists, or it has a new type, and we mark it as removed (see above)
                    diff.removed.put(entry.getKey(), type);
                }
            }
        }

        return diff;
    }

    @Override
    protected void destroy() {
        // Clear our session's script directory:
        if (classCacheDirectory.exists()) {
            FileUtils.deleteRecursively(classCacheDirectory);
        }
    }

    /**
     * Evaluates command in the context of the current ScriptSession.
     * @param command the command to evaluate
     * @param scriptName an optional script name, which may be ignored by the implementation, or used improve error
     *                   messages or for other internal purposes
     */
    protected abstract void evaluate(String command, @Nullable String scriptName);

    /**
     * @return a query scope for this session; only invoked during construction
     */
    protected abstract QueryScope newQueryScope();

    @Override
    @SuppressWarnings("rawtypes")
    public CompletionParseService getParser() {
        return parser;
    }


    @Override
    public Class getVariableType(final String var) {
        final Object result = getVariable(var, null);
        if (result == null) {
            return null;
        } else if (result instanceof Table) {
            return Table.class;
        } else {
            return result.getClass();
        }
    }


    @Override
    public TableDefinition getTableDefinition(final String var) {
        Object o = getVariable(var, null);
        return o instanceof Table ? ((Table) o).getDefinition() : null;
    }

    @Override
    public VariableProvider getVariableProvider() {
        return this;
    }
}
