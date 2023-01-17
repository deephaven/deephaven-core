/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import com.google.auto.service.AutoService;
import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;
import groovy.lang.MissingPropertyException;
import io.deephaven.base.FileUtils;
import io.deephaven.base.Pair;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompiler;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.util.GroovyDeephavenSession.GroovySnapshot;
import io.deephaven.engine.util.scripts.ScriptPathLoader;
import io.deephaven.engine.util.scripts.ScriptPathLoaderState;
import io.deephaven.engine.util.scripts.StateOverrideScriptPathLoader;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.util.annotations.VisibleForTesting;
import org.codehaus.groovy.control.CompilationUnit;
import org.codehaus.groovy.control.Phases;
import org.codehaus.groovy.tools.GroovyClass;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.tools.JavaFileObject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Groovy {@link ScriptSession}. Not safe for concurrent use.
 */
public class GroovyDeephavenSession extends AbstractScriptSession<GroovySnapshot> {
    private static final Logger log = LoggerFactory.getLogger(GroovyDeephavenSession.class);

    public static final String SCRIPT_TYPE = "Groovy";
    private static final String PACKAGE = QueryCompiler.DYNAMIC_GROOVY_CLASS_PREFIX;
    private static final String SCRIPT_PREFIX = "io.deephaven.engine.util.Script";

    private static final String DEFAULT_SCRIPT_PATH = Configuration.getInstance()
            .getStringWithDefault("GroovyDeephavenSession.defaultScriptPath", ".");

    private static final boolean ALLOW_UNKNOWN_GROOVY_PACKAGE_IMPORTS = Configuration.getInstance()
            .getBooleanForClassWithDefault(GroovyDeephavenSession.class, "allowUnknownGroovyPackageImports", false);

    private static final ClassLoader STATIC_LOADER =
            new URLClassLoader(new URL[0], GroovyDeephavenSession.class.getClassLoader()) {
                final ConcurrentHashMap<String, Object> mapping = new ConcurrentHashMap<>();

                @Override
                protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                    if (!mapping.containsKey(name)) {
                        try {
                            if (name.replaceAll("\\$", "\\.").contains(PACKAGE)) {
                                throw new ClassNotFoundException();
                            }
                            Class<?> aClass = super.loadClass(name, resolve);
                            mapping.put(name, aClass);
                            return aClass;
                        } catch (ClassNotFoundException e) {
                            mapping.put(name, e);
                            throw e;
                        }
                    } else {
                        Object obj = mapping.get(name);
                        if (obj instanceof Class) {
                            return (Class<?>) obj;
                        } else {
                            throw new ClassNotFoundException();
                        }
                    }
                }
            };

    private final ScriptFinder scriptFinder;

    private final ArrayList<String> scriptImports = new ArrayList<>();

    private final Set<String> dynamicClasses = new HashSet<>();
    private final GroovyShell groovyShell = new GroovyShell(STATIC_LOADER) {
        protected synchronized String generateScriptName() {
            return GroovyDeephavenSession.this.generateScriptName();
        }
    };

    private int counter;
    private String script = "Script";

    private String generateScriptName() {
        return script + "_" + (++counter) + ".groovy";
    }

    private String getNextScriptClassName() {
        return script + "_" + (counter + 1);
    }

    // the closures we have set for sourcing scripts
    private transient SourceClosure sourceClosure;
    private transient SourceClosure sourceOnceClosure;

    public GroovyDeephavenSession(ObjectTypeLookup objectTypeLookup, final RunScripts runScripts)
            throws IOException {
        this(objectTypeLookup, null, runScripts);
    }

    public GroovyDeephavenSession(
            ObjectTypeLookup objectTypeLookup,
            @Nullable final Listener changeListener,
            final RunScripts runScripts)
            throws IOException {
        super(objectTypeLookup, changeListener);

        this.scriptFinder = new ScriptFinder(DEFAULT_SCRIPT_PATH);

        groovyShell.setVariable("__groovySession", this);
        groovyShell.setVariable("DB_SCRIPT_PATH", DEFAULT_SCRIPT_PATH);

        executionContext.getQueryCompiler().setParentClassLoader(getShell().getClassLoader());

        publishInitial();

        for (final String path : runScripts.paths) {
            runScript(path);
        }
    }

    @Override
    public QueryScope newQueryScope() {
        return new SynchronizedScriptSessionQueryScope(this);
    }

    public static InputStream findScript(String relativePath) throws IOException {
        return new ScriptFinder(DEFAULT_SCRIPT_PATH).findScript(relativePath);
    }

    public void runScript(String script) throws IOException {
        final String dbScriptPath = (String) groovyShell.getVariable("DB_SCRIPT_PATH");
        final InputStream file = scriptFinder.findScript(script, dbScriptPath);
        final String scriptName = script.substring(0, script.indexOf("."));

        log.info("Executing script: " + script);
        evaluateScript(FileUtils.readTextFile(file), scriptName);
    }

    private final Set<String> executedScripts = new HashSet<>();

    // Used by closures that implement source() more directly to figure out if we've loaded a script already
    public boolean hasExecutedScript(final String scriptName) {
        return !executedScripts.add(scriptName);
    }

    public void runScriptOnce(String script) throws IOException {
        if (executedScripts.contains(script)) {
            return;
        }
        runScript(script);
        executedScripts.add(script);
    }

    @NotNull
    @Override
    public Object getVariable(String name) throws QueryScope.MissingVariableException {
        try {
            return groovyShell.getContext().getVariable(name);
        } catch (MissingPropertyException mpe) {
            throw new QueryScope.MissingVariableException("No binding for: " + name, mpe);
        }
    }

    @Override
    public <T> T getVariable(String name, T defaultValue) {
        try {
            // noinspection unchecked
            return (T) getVariable(name);
        } catch (QueryScope.MissingVariableException e) {
            return defaultValue;
        }
    }

    private void evaluateCommand(String command) {
        groovyShell.evaluate(command);
    }

    @Override
    protected void evaluate(String command, String scriptName) {
        grepScriptImports(removeComments(command));

        final Pair<String, String> fc = fullCommand(command);
        final String lastCommand = fc.second;
        final String commandPrefix = fc.first;

        final String oldScriptName = script;

        try {
            if (scriptName != null) {
                script = scriptName.replaceAll("[^0-9A-Za-z_]", "_").replaceAll("(^[0-9])", "_$1");
            }
            final String currentScriptName = script;

            updateClassloader(lastCommand);

            try {
                UpdateGraphProcessor.DEFAULT.exclusiveLock().doLockedInterruptibly(() -> evaluateCommand(lastCommand));
            } catch (InterruptedException e) {
                throw new CancellationException(e.getMessage() != null ? e.getMessage() : "Query interrupted",
                        maybeRewriteStackTrace(scriptName, currentScriptName, e, lastCommand, commandPrefix));
            } catch (Exception e) {
                throw wrapAndRewriteStackTrace(scriptName, currentScriptName, e, lastCommand, commandPrefix);
            }
        } finally {
            script = oldScriptName;
        }
    }

    private RuntimeException wrapAndRewriteStackTrace(String scriptName, String currentScriptName, Exception e,
            String lastCommand, String commandPrefix) {
        final Exception en = maybeRewriteStackTrace(scriptName, currentScriptName, e, lastCommand, commandPrefix);
        if (en instanceof RuntimeException) {
            return (RuntimeException) en;
        } else {
            return new RuntimeException(sanitizeThrowable(en));
        }
    }

    private Exception maybeRewriteStackTrace(String scriptName, String currentScriptName, Exception e,
            String lastCommand, String commandPrefix) {
        if (scriptName != null) {
            final StackTraceElement[] stackTrace = e.getStackTrace();
            for (int i = stackTrace.length - 1; i >= 0; i--) {
                final StackTraceElement stackTraceElement = stackTrace[i];
                if (stackTraceElement.getClassName().startsWith(PACKAGE + "." + currentScriptName) &&
                        stackTraceElement.getMethodName().equals("run")
                        && stackTraceElement.getFileName().endsWith(".groovy")) {
                    final String[] allLines = lastCommand.split("\n");
                    final int prefixLineCount = org.apache.commons.lang3.StringUtils.countMatches(commandPrefix, "\n");
                    final int userLineNumber = stackTraceElement.getLineNumber() - prefixLineCount;
                    if (stackTraceElement.getLineNumber() <= allLines.length) {
                        return new RuntimeException("Error encountered at line " + userLineNumber + ": "
                                + allLines[stackTraceElement.getLineNumber() - 1], sanitizeThrowable(e));
                    } else {
                        return new RuntimeException(
                                "Error encountered in Groovy script; unable to identify original line number.",
                                sanitizeThrowable(e));
                    }
                }
            }
        }
        return e;
    }

    private static String classForNameString(String className) throws ClassNotFoundException {
        try {
            Class.forName(className);
            return className;
        } catch (ClassNotFoundException e) {
            if (className.contains(".")) {
                // handle inner class cases
                int index = className.lastIndexOf('.');
                String head = className.substring(0, index);
                String tail = className.substring(index + 1);
                String newClassName = head + "$" + tail;

                return classForNameString(newClassName);
            } else {
                throw e;
            }
        }
    }

    private static boolean classExists(String className) {
        try {
            classForNameString(className);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static boolean functionExists(String className, String functionName) {
        try {
            Method[] ms = Class.forName(classForNameString(className)).getMethods();

            for (Method m : ms) {
                if (m.getName().equals(functionName)) {
                    return true;
                }
            }

            return false;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static boolean fieldExists(String className, String fieldName) {
        try {
            Field[] fs = Class.forName(classForNameString(className)).getFields();

            for (Field f : fs) {
                if (f.getName().equals(fieldName)) {
                    return true;
                }
            }

            return false;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Remove comments from an import statement. /* comments take precedence over eol (//) comments. This ignores
     * escaping and quoting, as they are not valid in an import statement.
     *
     * @param s import statement string from which to remove comments
     * @return the input string with comments removed, and whitespace trimmed
     */
    @VisibleForTesting
    public static String removeComments(String s) {
        // first remove /*...*/. This might include // comments, e.g. /* use // to comment to the end of the line */
        s = s.replaceAll("/(?s)\\*.*?\\*/", ""); // reluctant match inside /* */
        s = s.replaceFirst("//.*", "");

        return s.trim();
    }

    /**
     * Ensure that the given importString is valid. Return a canonical version of the import string if it is valid.
     *
     * @param importString the string to check. importString is "[import] [static]
     *        package.class[.innerclass...][.field|.method][.*][;]".
     * @return null if importString is not valid, else a string of the form "import [static]
     *         package.class.part.part[.*];"
     */
    @VisibleForTesting
    public static String isValidImportString(Logger log, String importString) {
        // look for (ignoring whitespace): optional "import" optional "static" everything_else optional ".*" optional
        // semicolon
        // "everything_else" should be a valid java identifier of the form package.class[.class|.method|.field]. This
        // will be checked later
        Matcher matcher = Pattern
                .compile("^\\s*(import\\s+)\\s*(?<static>static\\s+)?\\s*(?<body>.*?)(?<wildcard>\\.\\*)?[\\s;]*$")
                .matcher(importString);
        if (!matcher.matches()) {
            return null;
        }
        final boolean isStatic = matcher.group("static") != null;
        final boolean isWildcard = matcher.group("wildcard") != null;
        final String body = matcher.group("body");
        if (body == null) {
            return null;
        }

        boolean okToImport;
        if (isStatic) {
            if (isWildcard) {
                // import static package.class[.class].*
                okToImport = classExists(body);
            } else {
                // import static package.class.class
                // import static package.class[.class].method
                // import static package.class[.class].field
                final int lastSeparator = body.lastIndexOf(".");
                if (lastSeparator > 0) {
                    final String prefix = body.substring(0, lastSeparator);
                    final String suffix = body.substring(lastSeparator + 1);
                    okToImport = functionExists(prefix, suffix) || fieldExists(prefix, suffix) || classExists(body);
                } else {
                    okToImport = classExists(body);
                }
            }
        } else {
            if (isWildcard) {
                okToImport = classExists(body) || (Package.getPackage(body) != null); // Note: this might not find a
                                                                                      // valid package that has never
                                                                                      // been loaded
                if (!okToImport) {
                    if (ALLOW_UNKNOWN_GROOVY_PACKAGE_IMPORTS) {
                        // Check for proper form of a package. Pass a package star import that is plausible. Groovy is
                        // OK with packages that cannot be found, unlike java.
                        final String javaIdentifierPattern =
                                "(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*\\.)+\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";
                        if (body.matches(javaIdentifierPattern)) {
                            log.info().append("Package or class \"").append(body)
                                    .append("\" could not be verified. If this is a package, it could mean that no class from that package has been seen by the classloader.")
                                    .endl();
                            okToImport = true;
                        } else {
                            log.warn().append("Package or class \"").append(body)
                                    .append("\" could not be verified and does not appear to be a valid java identifier.")
                                    .endl();
                        }
                    } else {
                        log.warn().append("Package or class \"").append(body)
                                .append("\" could not be verified. If this is a package, it could mean that no class from that package has been seen by the classloader.")
                                .endl();
                    }
                }
            } else {
                okToImport = classExists(body);
            }
        }

        if (okToImport) {
            String fixedImport = "import " + (isStatic ? "static " : "") + body + (isWildcard ? ".*" : "") + ";";
            log.info().append("Adding persistent import ")
                    .append(isStatic ? "(static/" : "(normal/").append(isWildcard ? "wildcard): \"" : "normal): \"")
                    .append(fixedImport).append("\" from original string: \"").append(importString).append("\"").endl();
            return fixedImport;
        } else {
            log.error().append("Invalid import: \"").append(importString).append("\"").endl();
            return null;
        }
    }

    private void updateScriptImports(String importString) {
        String fixedImportString = isValidImportString(log, importString);
        if (fixedImportString != null) {
            scriptImports.add(importString);
        } else {
            throw new RuntimeException("Attempting to import a path that does not exist: " + importString);
        }
    }

    private void grepScriptImports(final String command) {
        for (String line : command.replace(";", "\n").split("\n")) {
            final String l = line.trim();

            if (l.startsWith("import ")) {
                log.info("Grepping script import: " + l);
                updateScriptImports(l + ";");
            }
        }
    }

    public void addScriptImportClass(String c) {
        log.info("Adding script class import: " + c);
        updateScriptImports("import " + c + ";");
    }

    public void addScriptImportClass(Class<?> c) {
        addScriptImportClass(c.getCanonicalName());
    }

    public void addScriptImportStatic(String c) {
        log.info("Adding script static import: " + c);
        updateScriptImports("import static " + c + ".*;");
    }

    public void addScriptImportStatic(Class<?> c) {
        addScriptImportStatic(c.getCanonicalName());
    }

    /**
     * Creates the full groovy command that we need to evaluate.
     *
     * Imports and the package line are added to the beginning; a postfix is added to the end. We return the prefix to
     * enable stack trace rewriting.
     *
     * @param command the user's input command
     * @return a pair of our command prefix (first) and the full command (second)
     */
    private Pair<String, String> fullCommand(String command) {
        // TODO (core#230): Remove large list of manual text-based imports
        // NOTE: Don't add to this list without a compelling reason!!! Use the user script import if possible.
        final String commandPrefix = "package " + PACKAGE + ";\n" +
                "import static io.deephaven.engine.util.TableTools.*;\n" +
                "import static io.deephaven.engine.table.impl.util.TableLoggers.*;\n" +
                "import static io.deephaven.engine.table.impl.util.PerformanceQueries.*;\n" +
                "import io.deephaven.api.*;\n" +
                "import io.deephaven.api.filter.*;\n" +
                "import io.deephaven.engine.table.DataColumn;\n" +
                "import io.deephaven.engine.table.Table;\n" +
                "import io.deephaven.engine.table.TableFactory;\n" +
                "import io.deephaven.engine.table.PartitionedTable;\n" +
                "import io.deephaven.engine.table.PartitionedTableFactory;\n" +
                "import java.lang.reflect.Array;\n" +
                "import io.deephaven.util.type.TypeUtils;\n" +
                "import io.deephaven.util.type.ArrayTypeUtils;\n" +
                "import io.deephaven.time.DateTime;\n" +
                "import io.deephaven.time.DateTimeUtils;\n" +
                "import io.deephaven.base.string.cache.CompressedString;\n" +
                "import static io.deephaven.base.string.cache.CompressedString.compress;\n" +
                "import org.joda.time.LocalTime;\n" +
                "import io.deephaven.time.Period;\n" +
                "import io.deephaven.engine.context.QueryScopeParam;\n" +
                "import io.deephaven.engine.context.QueryScope;\n" +
                "import java.util.*;\n" +
                "import java.lang.*;\n" +
                "import static io.deephaven.util.QueryConstants.*;\n" +
                "import static io.deephaven.libs.GroovyStaticImports.*;\n" +
                "import static io.deephaven.time.DateTimeUtils.*;\n" +
                "import static io.deephaven.time.TimeZone.*;\n" +
                "import static io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.*;\n" +
                "import static io.deephaven.api.agg.Aggregation.*;\n" +
                "import static io.deephaven.api.updateby.UpdateByOperation.*;\n" +

                String.join("\n", scriptImports) + "\n";
        return new Pair<>(commandPrefix, commandPrefix + command
                + "\n\n// this final true prevents Groovy from interpreting a trailing class definition as something to execute\n;\ntrue;\n");
    }

    public static byte[] getDynamicClass(String name) {
        return readClass(ExecutionContext.getContext().getQueryCompiler().getFakeClassDestination(), name);
    }

    private static byte[] readClass(final File rootDirectory, final String className) {
        final String resourceName = className.replace('.', '/') + JavaFileObject.Kind.CLASS.extension;
        final Path path = new File(rootDirectory, resourceName).toPath();
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new RuntimeException("Error reading path " + path + " for className " + className, e);
        }
    }

    private void updateClassloader(String currentCommand) {
        final String name = getNextScriptClassName();

        final CompilationUnit cu = new CompilationUnit(groovyShell.getClassLoader());
        cu.addSource(name, currentCommand);
        try {
            cu.compile(Phases.CLASS_GENERATION);
        } catch (RuntimeException e) {
            throw new GroovyExceptionWrapper(e);
        }
        final File dynamicClassDestination = ExecutionContext.getContext().getQueryCompiler().getFakeClassDestination();
        if (dynamicClassDestination == null) {
            return;
        }
        // noinspection unchecked
        final List<GroovyClass> classes = cu.getClasses();
        final Map<String, byte[]> newDynamicClasses = new HashMap<>();
        for (final GroovyClass aClass : classes) {
            // Exclude anonymous (numbered) dynamic classes
            if (aClass.getName().startsWith(SCRIPT_PREFIX)
                    && isAnInteger(aClass.getName().substring(SCRIPT_PREFIX.length()))) {
                continue;
            }
            // always put classes into the writable class loader, because it is possible that their content may have
            // changed
            newDynamicClasses.put(aClass.getName(), aClass.getBytes());
        }

        if (!newDynamicClasses.isEmpty()) {
            boolean notifiedQueryLibrary = false;
            for (final Map.Entry<String, byte[]> entry : newDynamicClasses.entrySet()) {
                // only increment QueryLibrary version if some dynamic class overrides an existing class
                if (!dynamicClasses.add(entry.getKey()) && !notifiedQueryLibrary) {
                    notifiedQueryLibrary = true;
                    executionContext.getQueryLibrary().updateVersionString();
                }

                try {
                    QueryCompiler.writeClass(dynamicClassDestination, entry.getKey(), entry.getValue());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * I factored out this horrible snippet of code from the updateClassLoader, to isolate the badness. I can't think of
     * a replacement that doesn't involve regex matching.
     *
     * @param s The string to evaluate
     * @return Whether s can be parsed as an int.
     */
    private static boolean isAnInteger(final String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public Map<String, Object> getVariables() {
        // noinspection unchecked
        return Collections.unmodifiableMap(groovyShell.getContext().getVariables());
    }

    @Override
    protected GroovySnapshot emptySnapshot() {
        return new GroovySnapshot(Collections.emptyMap());
    }

    @Override
    protected GroovySnapshot takeSnapshot() {
        // noinspection unchecked,rawtypes
        return new GroovySnapshot(new LinkedHashMap<>(groovyShell.getContext().getVariables()));
    }

    @Override
    protected Changes createDiff(GroovySnapshot from, GroovySnapshot to, RuntimeException e) {
        Changes diff = new Changes();
        diff.error = e;
        for (final Map.Entry<String, Object> entry : to.scope.entrySet()) {
            final String name = entry.getKey();
            final Object existingValue = from.scope.get(name);
            final Object newValue = entry.getValue();
            applyVariableChangeToDiff(diff, name, existingValue, newValue);
        }
        for (final Map.Entry<String, Object> entry : from.scope.entrySet()) {
            final String name = entry.getKey();
            if (to.scope.containsKey(name)) {
                continue; // this is already handled even if old or new values are non-displayable
            }
            applyVariableChangeToDiff(diff, name, entry.getValue(), null);
        }
        return diff;
    }

    protected static class GroovySnapshot implements Snapshot {

        private final Map<String, Object> scope;

        public GroovySnapshot(Map<String, Object> existingScope) {
            this.scope = Objects.requireNonNull(existingScope);
        }

        @Override
        public void close() {
            // no-op
        }
    }

    public Set<String> getVariableNames() {
        // noinspection unchecked
        return Collections.unmodifiableSet(groovyShell.getContext().getVariables().keySet());
    }

    @Override
    public boolean hasVariableName(String name) {
        return groovyShell.getContext().hasVariable(name);
    }

    @Override
    public void setVariable(String name, @Nullable Object newValue) {
        final Object oldValue = getVariable(name, null);
        groovyShell.getContext().setVariable(NameValidator.validateQueryParameterName(name), newValue);
        notifyVariableChange(name, oldValue, newValue);
    }

    public Binding getBinding() {
        return groovyShell.getContext();
    }

    public GroovyShell getShell() {
        return groovyShell;
    }

    @Override
    public String scriptType() {
        return SCRIPT_TYPE;
    }

    @Override
    public Throwable sanitizeThrowable(Throwable e) {
        return GroovyExceptionWrapper.maybeTranslateGroovyException(e);
    }

    @Override
    public void onApplicationInitializationBegin(Supplier<ScriptPathLoader> pathLoaderSupplier,
            ScriptPathLoaderState scriptLoaderState) {
        ExecutionContext.getContext().getQueryCompiler().setParentClassLoader(getShell().getClassLoader());
        setScriptPathLoader(pathLoaderSupplier, true);
    }

    @Override
    public void onApplicationInitializationEnd() {
        if (sourceClosure != null) {
            sourceClosure.clearCache();
        }
        if (sourceOnceClosure != null) {
            sourceOnceClosure.clearCache();
        }
    }

    @Override
    public void setScriptPathLoader(Supplier<ScriptPathLoader> pathLoaderSupplier, boolean caching) {
        final ScriptPathLoader pathLoader = pathLoaderSupplier.get();
        setVariable("source", sourceClosure = new SourceClosure(this, pathLoader, false, caching));
        setVariable("sourceOnce", sourceOnceClosure = new SourceClosure(this, pathLoader, true, false));
    }

    @Override
    public boolean setUseOriginalScriptLoaderState(boolean useOriginal) {
        final Object sourceClosure = getVariable("source");

        if (sourceClosure instanceof SourceClosure) {
            final ScriptPathLoader loader = ((SourceClosure) sourceClosure).getPathLoader();

            if (loader instanceof StateOverrideScriptPathLoader) {
                final StateOverrideScriptPathLoader sospl = (StateOverrideScriptPathLoader) loader;

                if (useOriginal) {
                    sospl.clearOverride();
                    final ScriptPathLoaderState scriptLoaderState = sospl.getUseState();
                    log.info().append("Using startup script loader state: ")
                            .append(scriptLoaderState == null ? "Latest" : scriptLoaderState.toString()).endl();
                } else {
                    log.info().append("Using latest script states").endl();
                    sospl.setOverrideState(ScriptPathLoaderState.NONE);
                }

                ((SourceClosure) sourceClosure).clearCache();

                return true;
            } else {
                log.warn().append("Incorrect loader type for query: ")
                        .append(loader == null ? "(null)" : loader.getClass().toString()).endl();
            }
        } else {
            log.warn().append("Incorrect closure type for query: ")
                    .append(sourceClosure.getClass().toString()).endl();
        }

        return false;
    }

    @Override
    public void clearScriptPathLoader() {
        Object sourceClosure = getVariable("source");
        if (sourceClosure instanceof SourceClosure) {
            ((SourceClosure) sourceClosure).getPathLoader().close();
        }

        setVariable("source", new SourceDisabledClosure(this));
        setVariable("sourceOnce", new SourceDisabledClosure(this));
    }

    private static class SourceDisabledClosure extends Closure<Object> {
        SourceDisabledClosure(GroovyDeephavenSession groovySession) {
            super(groovySession, null);
        }

        @Override
        public String call(Object... args) {
            throw new UnsupportedOperationException("This console does not support source.");
        }
    }

    public static class RunScripts {
        public static RunScripts of(Iterable<InitScript> initScripts) {
            List<String> paths = StreamSupport.stream(initScripts.spliterator(), false)
                    .sorted(Comparator.comparingInt(InitScript::priority))
                    .map(InitScript::getScriptPath)
                    .collect(Collectors.toList());
            return new RunScripts(paths);
        }

        public static RunScripts none() {
            return new RunScripts(Collections.emptyList());
        }

        public static RunScripts serviceLoader() {
            return of(ServiceLoader.load(InitScript.class));
        }

        public static RunScripts oldConfiguration() {
            return new RunScripts(Arrays
                    .asList(Configuration.getInstance().getProperty("GroovyDeephavenSession.initScripts").split(",")));
        }

        private final List<String> paths;

        public RunScripts(List<String> paths) {
            this.paths = Objects.requireNonNull(paths);
        }
    }

    public interface InitScript {
        String getScriptPath();

        int priority();
    }

    @AutoService(InitScript.class)
    public static class Base implements InitScript {
        @Override
        public String getScriptPath() {
            return "groovy/0-base.groovy";
        }

        @Override
        public int priority() {
            return 0;
        }
    }

    @AutoService(InitScript.class)
    public static class PerformanceQueries implements InitScript {
        @Override
        public String getScriptPath() {
            return "groovy/1-performance.groovy";
        }

        @Override
        public int priority() {
            return 1;
        }
    }

    @AutoService(InitScript.class)
    public static class Calendars implements InitScript {
        @Override
        public String getScriptPath() {
            return "groovy/2-calendars.groovy";
        }

        @Override
        public int priority() {
            return 2;
        }
    }

    @AutoService(InitScript.class)
    public static class CountMetrics implements InitScript {
        @Override
        public String getScriptPath() {
            return "groovy/4-count-metrics.groovy";
        }

        @Override
        public int priority() {
            return 4;
        }
    }

}
