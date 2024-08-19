//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import com.google.auto.service.AutoService;
import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyShell;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.updateby.BadDataBehavior;
import io.deephaven.api.updateby.DeltaControl;
import io.deephaven.api.updateby.OperationControl;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.FileUtils;
import io.deephaven.base.Pair;
import io.deephaven.engine.context.*;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.PartitionedTableFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableFactory;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.table.impl.util.TableLoggers;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.GroovyDeephavenSession.GroovySnapshot;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.libs.GroovyStaticImports;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.calendar.StaticCalendarMethods;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.util.type.TypeUtils;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import org.codehaus.groovy.control.CompilationUnit;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.Phases;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.codehaus.groovy.tools.GroovyClass;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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
    private static final String PACKAGE = QueryCompilerImpl.DYNAMIC_CLASS_PREFIX;
    private static final String SCRIPT_PREFIX = "io.deephaven.engine.util.Script";

    private static final String DEFAULT_SCRIPT_PATH = Configuration.getInstance()
            .getStringWithDefault("GroovyDeephavenSession.defaultScriptPath", ".");
    private static final String DEFAULT_SCRIPT_PREFIX = "Script";

    private static final boolean INCLUDE_DEFAULT_IMPORTS_IN_LOADED_GROOVY =
            Configuration.getInstance()
                    .getBooleanWithDefault("GroovyDeephavenSession.includeDefaultImportsInGroovyScripts", false);

    private static final boolean ALLOW_UNKNOWN_GROOVY_PACKAGE_IMPORTS = Configuration.getInstance()
            .getBooleanForClassWithDefault(GroovyDeephavenSession.class, "allowUnknownGroovyPackageImports", false);

    private static final ClassLoader STATIC_LOADER =
            new URLClassLoader(new URL[0], Thread.currentThread().getContextClassLoader()) {
                final ConcurrentHashMap<String, Object> mapping = new ConcurrentHashMap<>();

                @Override
                protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                    if (!mapping.containsKey(name)) {
                        try {
                            if (name.replaceAll("\\$", ".").contains(PACKAGE)) {
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

    /** Contains imports to be applied to commands run in the console */
    private final ImportCustomizer consoleImports;
    /** Contains imports to be applied to .groovy files loaded from the classpath */
    private final ImportCustomizer loadedGroovyScriptImports;

    private final Set<String> dynamicClasses;
    private final Map<String, Object> bindingBackingMap;

    private static class DeephavenGroovyShell extends GroovyShell {
        private final AtomicInteger counter = new AtomicInteger();
        private volatile String scriptPrefix = DEFAULT_SCRIPT_PREFIX;

        DeephavenGroovyShell(
                final GroovyClassLoader loader,
                final Binding binding,
                final CompilerConfiguration config) {
            super(loader, binding, config);
        }

        @Override
        protected String generateScriptName() {
            return scriptPrefix + "_" + (counter.incrementAndGet()) + ".groovy";
        }

        private String getNextScriptClassName() {
            return scriptPrefix + "_" + (counter.get() + 1);
        }

        public SafeCloseable setScriptPrefix(final String newPrefix) {
            scriptPrefix = newPrefix;
            return () -> {
                scriptPrefix = newPrefix;
            };
        }
    }

    private final DeephavenGroovyShell groovyShell;

    public static GroovyDeephavenSession of(
            final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer,
            final ObjectTypeLookup objectTypeLookup,
            final RunScripts runScripts) throws IOException {
        return GroovyDeephavenSession.of(updateGraph, operationInitializer, objectTypeLookup, null, runScripts);
    }

    public static GroovyDeephavenSession of(
            final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer,
            ObjectTypeLookup objectTypeLookup,
            @Nullable final Listener changeListener,
            final RunScripts runScripts) throws IOException {

        final ImportCustomizer consoleImports = new ImportCustomizer();
        final ImportCustomizer loadedGroovyScriptImports = new ImportCustomizer();

        addDefaultImports(consoleImports);
        if (INCLUDE_DEFAULT_IMPORTS_IN_LOADED_GROOVY) {
            addDefaultImports(loadedGroovyScriptImports);
        }

        final File classCacheDirectory = AbstractScriptSession.newClassCacheLocation().toFile();

        // Specify a classloader to read from the classpath, with script imports
        CompilerConfiguration scriptConfig = new CompilerConfiguration();
        scriptConfig.getCompilationCustomizers().add(loadedGroovyScriptImports);
        scriptConfig.setTargetDirectory(classCacheDirectory);
        GroovyClassLoader scriptClassLoader = new GroovyClassLoader(STATIC_LOADER, scriptConfig);

        // Specify a configuration for compiling/running console commands for custom imports
        CompilerConfiguration consoleConfig = new CompilerConfiguration();
        consoleConfig.getCompilationCustomizers().add(consoleImports);
        consoleConfig.setTargetDirectory(classCacheDirectory);

        Map<String, Object> bindingBackingMap = Collections.synchronizedMap(new LinkedHashMap<>());
        Binding binding = new Binding(bindingBackingMap);
        DeephavenGroovyShell groovyShell = new DeephavenGroovyShell(scriptClassLoader, binding, consoleConfig);


        return new GroovyDeephavenSession(updateGraph, operationInitializer, objectTypeLookup, changeListener,
                runScripts, classCacheDirectory, consoleImports, loadedGroovyScriptImports, bindingBackingMap,
                groovyShell);
    }

    private GroovyDeephavenSession(
            final UpdateGraph updateGraph,
            final OperationInitializer operationInitializer,
            ObjectTypeLookup objectTypeLookup,
            @Nullable final Listener changeListener,
            final RunScripts runScripts,
            final File classCacheDirectory,
            final ImportCustomizer consoleImports,
            final ImportCustomizer loadedGroovyScriptImports,
            final Map<String, Object> bindingBackingMap,
            final DeephavenGroovyShell groovyShell)
            throws IOException {
        super(updateGraph, operationInitializer, objectTypeLookup, changeListener, classCacheDirectory,
                groovyShell.getClassLoader());

        this.scriptFinder = new ScriptFinder(DEFAULT_SCRIPT_PATH);

        this.consoleImports = consoleImports;
        this.loadedGroovyScriptImports = loadedGroovyScriptImports;

        this.dynamicClasses = new HashSet<>();
        this.bindingBackingMap = bindingBackingMap;

        this.groovyShell = groovyShell;

        groovyShell.setVariable("__groovySession", this);
        groovyShell.setVariable("DB_SCRIPT_PATH", DEFAULT_SCRIPT_PATH);

        publishInitial();

        for (final String path : runScripts.paths) {
            runScript(path);
        }
    }

    /**
     * Adds the default imports that Groovy users assume to be present.
     */
    private static void addDefaultImports(ImportCustomizer imports) {
        // TODO (core#230): Remove large list of manual text-based consoleImports
        // NOTE: Don't add to this list without a compelling reason!!! Use the user script import if possible.
        imports.addImports(
                ColumnSource.class.getName(),
                RowSet.class.getName(),
                TrackingRowSet.class.getName(),
                Table.class.getName(),
                TableFactory.class.getName(),
                PartitionedTable.class.getName(),
                PartitionedTableFactory.class.getName(),
                Array.class.getName(),
                TypeUtils.class.getName(),
                ArrayTypeUtils.class.getName(),
                DateTimeUtils.class.getName(),
                Instant.class.getName(),
                LocalDate.class.getName(),
                LocalTime.class.getName(),
                ZoneId.class.getName(),
                ZonedDateTime.class.getName(),
                QueryScopeParam.class.getName(),
                QueryScope.class.getName(),
                UpdateByControl.class.getName(),
                OperationControl.class.getName(),
                DeltaControl.class.getName(),
                BadDataBehavior.class.getName(),
                ExecutionContext.class.getName());
        imports.addStarImports(
                "io.deephaven.api",
                "io.deephaven.api.filter",
                "java.util",
                "java.lang");
        imports.addStaticStars(
                TableTools.class.getName(),
                TableLoggers.class.getName(),
                QueryConstants.class.getName(),
                GroovyStaticImports.class.getName(),
                DateTimeUtils.class.getName(),
                QueryLanguageFunctionUtils.class.getName(),
                Aggregation.class.getName(),
                UpdateByOperation.class.getName(),
                io.deephaven.time.calendar.Calendars.class.getName(),
                StaticCalendarMethods.class.getName());
    }

    public static InputStream findScript(String relativePath) throws IOException {
        return new ScriptFinder(DEFAULT_SCRIPT_PATH).findScript(relativePath);
    }

    public void runScript(String script) throws IOException {
        final String dbScriptPath = (String) groovyShell.getVariable("DB_SCRIPT_PATH");
        final InputStream file = scriptFinder.findScript(script, dbScriptPath);
        final String scriptName = script.substring(0, script.indexOf("."));

        log.info("Executing script: " + script);
        evaluateScript(FileUtils.readTextFile(file), scriptName).throwIfError();
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

    @Override
    protected <T> T getVariable(String name) {
        synchronized (bindingBackingMap) {
            if (bindingBackingMap.containsKey(name)) {
                // noinspection unchecked
                return (T) bindingBackingMap.get(name);
            }
            throw new QueryScope.MissingVariableException("Missing variable " + name);
        }
    }

    @Override
    protected void evaluate(String command, String scriptName) {
        grepScriptImports(removeComments(command));

        final Pair<String, String> fc = fullCommand(command);
        final String lastCommand = fc.second;
        final String commandPrefix = fc.first;

        final String currentScriptName = scriptName == null
                ? DEFAULT_SCRIPT_PREFIX
                : scriptName.replaceAll("[^0-9A-Za-z_]", "_").replaceAll("(^[0-9])", "_$1");
        try (final SafeCloseable ignored = groovyShell.setScriptPrefix(currentScriptName)) {

            updateClassloader(lastCommand);

            try {
                ExecutionContext.getContext().getUpdateGraph().exclusiveLock()
                        .doLockedInterruptibly(() -> groovyShell.evaluate(lastCommand));
            } catch (InterruptedException e) {
                throw new CancellationException(e.getMessage() != null ? e.getMessage() : "Query interrupted",
                        maybeRewriteStackTrace(scriptName, currentScriptName, e, lastCommand, commandPrefix));
            } catch (Exception e) {
                throw wrapAndRewriteStackTrace(scriptName, currentScriptName, e, lastCommand, commandPrefix);
            }
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

    private Class<?> loadClass(String className) throws ClassNotFoundException {
        try {
            return Class.forName(className, false, this.groovyShell.getClassLoader());
        } catch (ClassNotFoundException e) {
            if (className.contains(".")) {
                // handle inner class cases
                int index = className.lastIndexOf('.');
                String head = className.substring(0, index);
                String tail = className.substring(index + 1);
                String newClassName = head + "$" + tail;

                return loadClass(newClassName);
            } else {
                throw e;
            }
        }
    }

    private boolean classExists(String className) {
        try {
            loadClass(className);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private boolean functionExists(String className, String functionName) {
        try {
            Method[] ms = loadClass(className).getMethods();

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

    private boolean fieldExists(String className, String fieldName) {
        try {
            Field[] fs = loadClass(className).getFields();

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
     * Represents an import that can be added to an ImportCustomizer, as a valid return from
     * {@link #createImport(String)}.
     */
    @VisibleForTesting
    public interface GroovyImport {
        void appendTo(ImportCustomizer imports);
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
    public Optional<GroovyImport> createImport(String importString) {
        // look for (ignoring whitespace):
        // "import" optional "static" qualified_name optional ".*" optional "as" optional name optional semicolon
        //
        // "qualified_name" should be a valid java qualified name, consisting of "."-separated java identifiers. "name"
        // should be a valid java identifier. These will be checked later by Groovy.
        Matcher matcher = Pattern
                .compile(
                        "^\\s*(import\\s+)\\s*(?<static>static\\s+)?\\s*(?<body>.*?)(?<wildcard>\\.\\*)?(\\s+as\\s+(?<alias>.*?))?[\\s;]*$")
                .matcher(importString);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        final boolean isStatic = matcher.group("static") != null;
        final boolean isWildcard = matcher.group("wildcard") != null;
        final String body = matcher.group("body");
        @Nullable
        final String alias = matcher.group("alias");
        if (body == null || (isWildcard && alias != null)) {
            // Can't build an import without something to import, and can't alias a wildcard
            return Optional.empty();
        }

        if (isStatic) {
            return createStaticImport(isWildcard, body, alias);
        }
        return createClassImport(isWildcard, body, alias);
    }

    private Optional<GroovyImport> createStaticImport(boolean isWildcard, String body, @Nullable String alias) {
        if (isWildcard) {
            // import static package.class[.class].*
            if (!classExists(body)) {
                return Optional.empty();
            }
            return Optional.of(imports -> imports.addStaticStars(body));
        }
        // import static package.class.class
        // import static package.class[.class].method
        // import static package.class[.class].field
        final int lastSeparator = body.lastIndexOf(".");
        final String typeName;
        @Nullable
        final String memberName;
        if (lastSeparator > 0) {
            typeName = body.substring(0, lastSeparator);
            memberName = body.substring(lastSeparator + 1);
            if (!functionExists(typeName, memberName) && !fieldExists(typeName, memberName)
                    && !classExists(body)) {
                return Optional.empty();
            }
        } else {
            if (!classExists(body)) {
                return Optional.empty();
            }
            typeName = body;
            memberName = null;
        }
        if (alias == null) {
            return Optional.of(imports -> imports.addStaticImport(typeName, memberName));
        }
        return Optional.of(imports -> imports.addStaticImport(alias, typeName, memberName));
    }

    private Optional<GroovyImport> createClassImport(boolean isWildcard, String body, @Nullable String alias) {
        if (isWildcard) {
            if (classExists(body) || (groovyShell.getClassLoader().getDefinedPackage(body) != null)
                    || packageIsVisibleToClassGraph(body)) {
                return Optional.of(imports -> imports.addStarImports(body));
            }
            if (ALLOW_UNKNOWN_GROOVY_PACKAGE_IMPORTS) {
                // Check for proper form of a package. Pass a package star import that is plausible. Groovy is
                // OK with packages that cannot be found, unlike java.
                final String javaIdentifierPattern =
                        "(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*\\.)+\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";
                if (body.matches(javaIdentifierPattern)) {
                    log.info().append("Package or class \"").append(body)
                            .append("\" could not be verified.")
                            .endl();
                    return Optional.of(imports -> imports.addStarImports(body));
                }
                log.warn().append("Package or class \"").append(body)
                        .append("\" could not be verified and does not appear to be a valid java identifier.")
                        .endl();
                return Optional.empty();
            }
            log.warn().append("Package or class \"").append(body)
                    .append("\" could not be verified.")
                    .endl();
            return Optional.empty();
        } else {
            if (!classExists(body)) {
                return Optional.empty();
            }
            if (alias == null) {
                return Optional.of(imports -> imports.addImports(body));
            }
            return Optional.of(imports -> imports.addImport(alias, body));
        }
    }

    private static boolean packageIsVisibleToClassGraph(String packageImport) {
        try (ScanResult scanResult =
                new ClassGraph().enableClassInfo().enableSystemJarsAndModules().acceptPackages(packageImport).scan()) {
            final Optional<ClassInfo> firstClassFound = scanResult.getAllClasses().stream().findFirst();
            // force load the class so that the jvm is aware of the package
            firstClassFound.ifPresent(ClassInfo::loadClass);
            return firstClassFound.isPresent();
        }
    }

    private void updateScriptImports(String importString) {
        Optional<GroovyImport> validated = createImport(importString);
        if (validated.isPresent()) {
            log.info().append("Adding persistent import \"").append(importString).append("\"").endl();
            validated.get().appendTo(consoleImports);
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
     * <p>
     * Imports and the package line are added to the beginning; a postfix is added to the end. We return the prefix to
     * enable stack trace rewriting.
     *
     * @param command the user's input command
     * @return a pair of our command prefix (first) and the full command (second)
     */
    private Pair<String, String> fullCommand(String command) {
        final String commandPrefix = "package " + PACKAGE + ";\n";
        return new Pair<>(commandPrefix, commandPrefix + command
                + "\n\n// this final true prevents Groovy from interpreting a trailing class definition as something to execute\n;\ntrue;\n");
    }

    private void updateClassloader(String currentCommand) {
        final String name = groovyShell.getNextScriptClassName();

        CompilerConfiguration config = new CompilerConfiguration(CompilerConfiguration.DEFAULT);
        config.setTargetDirectory(classCacheDirectory);
        config.getCompilationCustomizers().add(consoleImports);
        final CompilationUnit cu = new CompilationUnit(config, null, groovyShell.getClassLoader());
        cu.addSource(name, currentCommand);

        try {
            cu.compile(Phases.CLASS_GENERATION);
        } catch (RuntimeException e) {
            throw new GroovyExceptionWrapper(e);
        }
        if (classCacheDirectory == null) {
            return;
        }
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
                    QueryCompilerImpl.writeClass(classCacheDirectory, entry.getKey(), entry.getValue());
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

    @Override
    protected Set<String> getVariableNames() {
        synchronized (bindingBackingMap) {
            return new HashSet<>(bindingBackingMap.keySet());
        }
    }

    @Override
    protected boolean hasVariable(String name) {
        return bindingBackingMap.containsKey(name);
    }

    @Override
    protected Object setVariable(String name, @Nullable Object newValue) {
        Object oldValue = bindingBackingMap.put(name, newValue);

        // Observe changes from this "setVariable" (potentially capturing previous or concurrent external changes from
        // other threads)
        observeScopeChanges();
        return oldValue;
    }

    @Override
    protected <T> Map<String, T> getAllValues(
            @Nullable final Function<Object, T> valueMapper,
            @NotNull final QueryScope.ParamFilter<T> filter) {
        final Map<String, T> result = new HashMap<>();

        synchronized (bindingBackingMap) {
            for (final Map.Entry<String, Object> entry : bindingBackingMap.entrySet()) {
                final String name = entry.getKey();
                Object value = entry.getValue();
                if (valueMapper != null) {
                    value = valueMapper.apply(value);
                }

                // noinspection unchecked
                if (filter.accept(name, (T) value)) {
                    // noinspection unchecked
                    result.put(name, (T) value);
                }
            }
        }

        return result;
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
