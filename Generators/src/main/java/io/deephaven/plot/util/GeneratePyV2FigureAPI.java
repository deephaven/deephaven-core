//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.util;

import io.deephaven.base.Pair;
import io.deephaven.gen.GenUtils;
import io.deephaven.gen.JavaFunction;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Generate the plotting Figure API for Python V2.
 */
public class GeneratePyV2FigureAPI {

    private static final String INDENT = "    ";
    private static final String JCLASS = "io.deephaven.plot.Figure";
    private static final String PYPREAMBLE = "/Generators/src/main/java/io/deephaven/pythonPreambles/plotV2.py";
    private static final String PYMODUlE = "/py/server/deephaven/plot/figure.py";
    private static String figureWrapperPreamble;
    private static String devroot;
    private static boolean assertNoChange;
    private static String plotPreamble;
    private static String plotOutput;
    private static String figureWrapperOutput;


    public static void main(String[] args) throws ClassNotFoundException, IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("args[0] = devroot, args[1] = assertNoChange");
        }

        devroot = args[0];
        assertNoChange = Boolean.parseBoolean(args[1]);
        figureWrapperPreamble = devroot + PYPREAMBLE;
        figureWrapperOutput = devroot + PYMODUlE;

        final Map<Key, ArrayList<JavaFunction>> signatures = getMethodSignatures();
        final int nSig1 = signatures.size();

        // generatePy removes values from signatures that it uses to generate code
        // as a result the signatures left over after calling generatePy are cases that
        // do not have generators or excludes and need to be handled
        String pyCode = generatePy(signatures);
        final int nSig2 = signatures.size();
        final int nSigGenerated = nSig1 - nSig2;

        System.out
                .println("GENSTATS: " + nSigGenerated + " of " + nSig1 + "(" + (nSigGenerated / (double) nSig1) + ")");

        if (!signatures.isEmpty()) {
            printMethodSignatures(signatures);
            throw new RuntimeException("Not all methods were generated.");
        }

        final File pythonFile = new File(figureWrapperOutput);

        if (!pythonFile.exists()) {
            throw new RuntimeException("File: " + figureWrapperOutput + " does not exist.");
        }

        if (assertNoChange) {
            String oldCode = new String(Files.readAllBytes(Paths.get(figureWrapperOutput)));
            GenUtils.assertGeneratedCodeSame(GeneratePyV2FigureAPI.class, ":Generators:generatePythonFigureWrapper",
                    oldCode, pyCode);
        } else {
            try (final PrintWriter out = new PrintWriter(pythonFile)) {
                out.print(pyCode);
            }
        }
    }

    /**
     * Java method signatures that are excluded from analysis.
     * <p>
     * Key = java function name Value = array of java function signatures to exclude. The signatures are lists of
     * parameter names. The order matters.
     */
    private static final Map<String, String[][]> javaExcludes = new HashMap<>() {
        {
            put("invert", new String[][] {new String[] {}});
            put("log", new String[][] {new String[] {}});
            put("businessTime", new String[][] {new String[] {}});
            put("xInvert", new String[][] {new String[] {}});
            put("xLog", new String[][] {new String[] {}});
            put("xBusinessTime", new String[][] {new String[] {}});
            put("yInvert", new String[][] {new String[] {}});
            put("yLog", new String[][] {new String[] {}});
            put("yBusinessTime", new String[][] {new String[] {}});
            put("axisLabelFont", new String[][] {new String[] {"family", "style", "size"}});
            put("figureTitleFont", new String[][] {new String[] {"family", "style", "size"}});
            put("chartTitleFont", new String[][] {new String[] {"family", "style", "size"}});
            put("legendFont", new String[][] {new String[] {"family", "style", "size"}});
            put("ticksFont", new String[][] {new String[] {"family", "style", "size"}});
            put("xLabelFont", new String[][] {new String[] {"family", "style", "size"}});
            put("xTicksFont", new String[][] {new String[] {"family", "style", "size"}});
            put("yLabelFont", new String[][] {new String[] {"family", "style", "size"}});
            put("yTicksFont", new String[][] {new String[] {"family", "style", "size"}});
            put("xTicksFont", new String[][] {new String[] {"family", "style", "size"}});
            put("xTicksFont", new String[][] {new String[] {"family", "style", "size"}});
            put("xTicksFont", new String[][] {new String[] {"family", "style", "size"}});
        }
    };

    /**
     * Gets the method signatures of public JCLASS methods.
     *
     * @return Map of method keys to a list of all relevant method signatures.
     * @throws ClassNotFoundException JCLASS is not found
     */
    public static Map<Key, ArrayList<JavaFunction>> getMethodSignatures() throws ClassNotFoundException {
        final Class<?> c = Class.forName(JCLASS, false, Thread.currentThread().getContextClassLoader());
        final Map<Key, ArrayList<JavaFunction>> signatures = new TreeMap<>();

        for (final Method m : c.getMethods()) {
            if (!m.getReturnType().getTypeName().equals(JCLASS)) {
                // only look at methods of the plot builder
                continue;
            }

            final Key key = new Key(m);
            final JavaFunction f = new JavaFunction(m);

            if (javaExcludes.containsKey(key.name)) {
                final boolean isExcluded = Arrays.stream(javaExcludes.get(key.name))
                        .anyMatch(exclude -> Arrays.equals(exclude, f.getParameterNames()));

                if (isExcluded) {
                    continue;
                }
            }

            if (key.isPublic) {
                final ArrayList<JavaFunction> sigs = signatures.computeIfAbsent(key, k -> new ArrayList<>());
                sigs.add(f);
            }
        }

        return signatures;
    }

    /**
     * Descriptive string for a method signature.
     *
     * @param key signature key
     * @param signatures list of signatures
     * @return descriptive string for a method signature
     */
    private static String describeMethodSignatures(final Key key, final ArrayList<JavaFunction> signatures) {
        final StringBuilder sb = new StringBuilder();

        sb.append("-----------------------------------------------------------------------\n");

        sb.append("Name: ").append(key.name).append("\n");
        sb.append("IsPublic: ").append(key.isPublic).append("\n");
        sb.append("IsStatic: ").append(key.isStatic).append("\n");

        final Set<String> returnTypes = new TreeSet<>();
        final Map<String, Set<String>> params = new TreeMap<>();

        for (final JavaFunction f : signatures) {
            returnTypes.add(f.getReturnType().getTypeName());

            for (int i = 0; i < f.getParameterNames().length; i++) {
                final Set<String> paramTypes = params.computeIfAbsent(f.getParameterNames()[i], n -> new TreeSet<>());
                paramTypes.add(f.getParameterTypes()[i].getTypeName());
            }
        }

        sb.append("ReturnTypes:\n");

        for (String returnType : returnTypes) {
            sb.append("\t").append(returnType).append("\n");
        }

        sb.append("Params:\n");

        for (Map.Entry<String, Set<String>> entry : params.entrySet()) {
            sb.append("\t").append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }

        sb.append("Signatures:\n");

        for (final JavaFunction f : signatures) {
            sb.append("\t");
            sb.append(f.getReturnType().getTypeName());
            sb.append(" (");

            for (int i = 0; i < f.getParameterNames().length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }

                sb.append(f.getParameterNames()[i]).append("=").append(f.getParameterTypes()[i].getTypeName());
            }

            sb.append(")\n");
        }

        return sb.toString();
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Type of function call.
     */
    private enum FunctionCallType {
        /**
         * Exactly one java function call is executed.
         */
        SINGLETON,
        /**
         * Multiple java function calls are sequentially executed.
         */
        SEQUENTIAL,
    }


    /**
     * A Key for indexing common method names.
     */
    private static class Key implements Comparable<Key> {
        private final String name;
        private final boolean isStatic;
        private final boolean isPublic;


        public Key(final Method m) {
            this.name = m.getName();
            this.isStatic = Modifier.isStatic(m.getModifiers());
            this.isPublic = Modifier.isPublic(m.getModifiers());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Key key = (Key) o;
            return isStatic == key.isStatic && isPublic == key.isPublic && Objects.equals(name, key.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, isStatic, isPublic);
        }

        @Override
        public String toString() {
            return "Key{" +
                    "name='" + name + '\'' +
                    ", isStatic=" + isStatic +
                    ", isPublic=" + isPublic +
                    '}';
        }

        @Override
        public int compareTo(@NotNull Key o) {
            final int c1 = this.name.compareTo(o.name);

            if (c1 != 0) {
                return c1;
            }

            if (this.isStatic != o.isStatic) {
                if (this.isStatic) {
                    return 1;
                } else {
                    return -1;
                }
            }

            if (this.isPublic != o.isPublic) {
                if (this.isPublic) {
                    return 1;
                } else {
                    return -1;
                }
            }

            return 0;
        }
    }


    /**
     * A Python method argument.
     */
    private static class PyArg implements Comparable<PyArg> {
        private final int precedence;
        private final String name;
        private final String[] typeAnnotations;
        private final String docString;
        private final String javaConverter;

        public PyArg(final int precedence, final String name, final String[] typeAnnotations, final String docString,
                final String javaConverter) {
            this.precedence = precedence;
            this.name = name;
            this.typeAnnotations = typeAnnotations;
            this.docString = docString;
            this.javaConverter = javaConverter == null ? "_convert_j" : javaConverter;
        }

        @Override
        public String toString() {
            return "PyArg{" +
                    "precedence=" + precedence +
                    ", name='" + name + '\'' +
                    ", typeAnnotations='" + Arrays.toString(typeAnnotations) + '\'' +
                    ", docString='" + docString + '\'' +
                    ", javaConverter='" + javaConverter + '\'' +
                    '}';
        }

        @Override
        public int compareTo(@NotNull PyArg o) {
            final int c1 = Integer.compare(this.precedence, o.precedence);

            if (c1 != 0) {
                return c1;
            }

            return this.name.compareTo(o.name);
        }

        /**
         * Returns the type annotation string.
         *
         * @return type annotation string
         */
        public String typeAnnotation() {
            return typeAnnotations.length == 1 ? typeAnnotations[0]
                    : "Union[" + String.join(", ", typeAnnotations) + "]";
        }

        /**
         * Returns a list of types string.
         *
         * @return list of types string
         */
        public String typeList() {
            return "[" + String.join(", ", typeAnnotations) + "]";
        }
    }

    /**
     * A Python function.
     */
    private static class PyFunc implements Comparable<PyFunc> {
        private final String name;
        private final FunctionCallType functionCallType;
        private final String[] javaFuncs;
        private final String[] requiredParams;
        private final String[] nullableParams;
        private final String pydoc;
        private final boolean generate;

        public PyFunc(final String name, final FunctionCallType functionCallType, final String[] javaFuncs,
                final String[] requiredParams, final String[] nullableParams, final String pydoc,
                final boolean generate) {
            this.name = name;
            this.functionCallType = functionCallType;
            this.javaFuncs = javaFuncs;
            this.requiredParams = requiredParams == null ? new String[] {} : requiredParams;
            this.nullableParams = nullableParams == null ? new String[] {} : nullableParams;
            this.pydoc = pydoc;
            this.generate = generate;
        }

        public PyFunc(final String name, final FunctionCallType functionCallType, final String[] javaFuncs,
                final String[] requiredParams, final String pydoc, final boolean generate) {
            this(name, functionCallType, javaFuncs, requiredParams, null, pydoc, generate);
        }

        public PyFunc(final String name, final FunctionCallType functionCallType, final String[] javaFuncs,
                final String[] requiredParams, final String pydoc) {
            this(name, functionCallType, javaFuncs, requiredParams, pydoc, true);
        }

        public PyFunc(final String name, final FunctionCallType functionCallType, final String[] javaFuncs,
                final String[] requiredParams, final String[] nullableParams, final String pydoc) {
            this(name, functionCallType, javaFuncs, requiredParams, nullableParams, pydoc, true);
        }

        @Override
        public String toString() {
            return "PyFunc{" +
                    "name='" + name + '\'' +
                    ", functionCallType=" + functionCallType +
                    ", javaFuncs=" + Arrays.toString(javaFuncs) +
                    ", requiredParams=" + Arrays.toString(requiredParams) +
                    ", pydoc='" + pydoc + '\'' +
                    ", generate=" + generate +
                    '}';
        }

        @Override
        public int compareTo(@NotNull PyFunc o) {
            return this.name.compareTo(o.name);
        }

        /**
         * Gets the Java signatures for this method.
         * <p>
         * Signatures are sorted based upon the order in the input javaFuncs.
         *
         * @param signatures java signatures
         * @return Java signatures for this method.
         */
        public Map<Key, ArrayList<JavaFunction>> getSignatures(final Map<Key, ArrayList<JavaFunction>> signatures) {
            final Map<String, Integer> order = IntStream.range(0, javaFuncs.length)
                    .boxed()
                    .collect(Collectors.toMap(i -> javaFuncs[i], i -> i));

            final Map<Key, ArrayList<JavaFunction>> rst =
                    new TreeMap<>(Comparator.comparingInt(a -> order.get(a.name)));

            rst.putAll(
                    signatures
                            .entrySet()
                            .stream()
                            .filter(e -> order.containsKey(e.getKey().name))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

            if (rst.size() != javaFuncs.length) {
                final Set<String> required = new TreeSet<>(Arrays.asList(javaFuncs));
                final Set<String> actual = rst.keySet().stream().map(k -> k.name).collect(Collectors.toSet());
                required.removeAll(actual);
                throw new IllegalArgumentException(
                        "Java methods are not present in signatures: func=" + this + " methods=" + required);
            }

            return rst;
        }

        /**
         * Is the parameter required for the function?
         *
         * @param parameter python parameter
         * @return is the parameter required for the function?
         */
        public boolean isRequired(final PyArg parameter) {
            return Arrays.asList(requiredParams).contains(parameter.name);
        }

        /**
         * Is the parameter nullable for the function?
         *
         * @param parameter python parameter
         * @return is the parameter nullable for the function?
         */
        public boolean isNullable(final PyArg parameter) {
            return Arrays.asList(nullableParams).contains(parameter.name);
        }

        /**
         * Gets the valid Java method argument name combinations.
         *
         * @param signatures java functions with the same name.
         * @return valid Java method argument name combinations.
         */
        private static Collection<String[]> javaArgNames(final ArrayList<JavaFunction> signatures) {
            final Map<Set<String>, String[]> vals = new LinkedHashMap<>();

            for (JavaFunction f : signatures) {
                final String[] params = f.getParameterNames();
                final Set<String> s = new HashSet<>(Arrays.asList(params));

                if (vals.containsKey(s) && !Arrays.equals(params, vals.get(s))) {
                    throw new RuntimeException("Parameters are already present: " + Arrays.toString(params));
                }

                vals.put(s, params);
            }

            return vals.values();
        }

        /**
         * Gets the valid Python method argument name combinations.
         *
         * @param signatures java functions with the same name.
         * @param pyArgMap possible python function arguments
         * @return valid Java method argument name combinations.
         */
        private static List<String[]> pyArgNames(final ArrayList<JavaFunction> signatures,
                final Map<String, PyArg> pyArgMap) {
            return pyArgNames(signatures, pyArgMap, new String[] {});
        }

        /**
         * Gets the valid Python method argument name combinations.
         *
         * @param signatures java functions with the same name.
         * @param pyArgMap possible python function arguments
         * @param excludeArgs arguments to exclude from the output
         * @return valid Java method argument name combinations.
         */
        private static List<String[]> pyArgNames(final ArrayList<JavaFunction> signatures,
                final Map<String, PyArg> pyArgMap, String[] excludeArgs) {
            final Set<Set<String>> seen = new HashSet<>();
            return javaArgNames(signatures)
                    .stream()
                    .map(an -> Arrays.stream(an).map(s -> pyArgMap.get(s).name)
                            .filter(s -> !Arrays.stream(excludeArgs).anyMatch(ex -> ex.equals(s)))
                            .toArray(String[]::new))
                    .filter(an -> seen.add(new HashSet<>(Arrays.asList(an))))
                    .sorted((first, second) -> {
                        final int c1 = Integer.compare(first.length, second.length);

                        if (c1 != 0) {
                            return c1;
                        }

                        for (int i = 0; i < first.length; i++) {
                            final int c2 = first[i].compareTo(second[i]);
                            if (c2 != 0) {
                                return c2;
                            }
                        }

                        return 0;
                    })
                    .collect(Collectors.toList());
        }

        /**
         * Gets the complete set of arguments for the python function.
         *
         * @param sigs java functions
         * @return complete set of arguments for the python function.
         */
        private List<PyArg> pyArgs(final Map<Key, ArrayList<JavaFunction>> sigs) {
            final Map<String, PyArg> pyparams = getPyArgs();
            final Set<PyArg> argSet = new HashSet<>();

            for (final ArrayList<JavaFunction> signatures : sigs.values()) {
                for (final JavaFunction f : signatures) {
                    for (final String param : f.getParameterNames()) {
                        final PyArg pyparam = pyparams.get(param);

                        if (pyparam == null) {
                            throw new IllegalArgumentException(
                                    "Unsupported python parameter: func=" + this + " param=" + param);
                        }

                        argSet.add(pyparam);
                    }
                }
            }

            final List<PyArg> args = new ArrayList<>(argSet);
            args.sort((a, b) -> {
                final boolean ra = isRequired(a);
                final boolean rb = isRequired(b);

                return ra != rb ? (ra ? -1 : 1) : a.compareTo(b);
            });

            return args;
        }

        /**
         * Generates code for the python function definition.
         *
         * @param args function arguments
         * @return code for the python function definition.
         */
        private String generatePyFuncDef(final List<PyArg> args) {
            final StringBuilder sb = new StringBuilder();

            sb.append(INDENT).append("def ").append(name).append("(\n");
            sb.append(INDENT).append(INDENT).append("self,\n");

            for (final PyArg arg : args) {
                sb.append(INDENT).append(INDENT).append(arg.name).append(": ").append(arg.typeAnnotation());

                if (!isRequired(arg)) {
                    sb.append(" = None");
                }

                sb.append(",\n");
            }

            sb.append(INDENT).append(") -> Figure:\n");

            return sb.toString();
        }

        /**
         * Generates code for the python function doc string.
         *
         * @param args function arguments
         * @return code for the python function doc string.
         */
        private String generatePyDocString(final List<PyArg> args) {
            final StringBuilder sb = new StringBuilder();

            sb
                    .append(INDENT)
                    .append(INDENT)
                    .append("\"\"\"")
                    .append(pydoc)
                    .append("\n\n");

            sb
                    .append(INDENT)
                    .append(INDENT)
                    .append("Args:\n");

            for (final PyArg arg : args) {
                sb
                        .append(INDENT)
                        .append(INDENT)
                        .append(INDENT)
                        .append(arg.name)
                        .append(" (")
                        .append(arg.typeAnnotation())
                        .append("): ")
                        .append(arg.docString)
                        .append("\n");
            }

            sb
                    .append("\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append("Returns:\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append(INDENT)
                    .append("a new Figure\n");

            sb
                    .append("\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append("Raises:\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append(INDENT)
                    .append("DHError\n");

            sb
                    .append(INDENT)
                    .append(INDENT)
                    .append("\"\"\"\n");

            return sb.toString();
        }

        /**
         * Generates code for the python function body.
         *
         * @param args function arguments
         * @param signatures java functions
         * @return code for the python function body.
         */
        private String generatePyFuncBody(final List<PyArg> args, final Map<Key, ArrayList<JavaFunction>> signatures) {
            final Map<String, PyArg> pyArgMap = getPyArgs();

            final StringBuilder sb = new StringBuilder();

            // validate

            for (final PyArg arg : args) {
                if (!isRequired(arg)) {
                    continue;
                }

                sb.append(INDENT)
                        .append(INDENT)
                        .append("if not ")
                        .append(arg.name)
                        .append(":\n")
                        .append(INDENT)
                        .append(INDENT)
                        .append(INDENT)
                        .append("raise DHError(\"required parameter is not set: ")
                        .append(arg.name)
                        .append("\")\n");
            }

            // non-null params

            sb.append(INDENT).append(INDENT).append("non_null_args = set()\n\n");

            for (final PyArg arg : args) {
                sb.append(INDENT)
                        .append(INDENT)
                        .append("if ")
                        .append(arg.name)
                        .append(" is not None:\n")
                        .append(INDENT)
                        .append(INDENT)
                        .append(INDENT)
                        .append("non_null_args.add(\"")
                        .append(arg.name).append("\")\n")
                        .append(INDENT)
                        .append(INDENT)
                        .append(INDENT)
                        .append(arg.name)
                        .append(" = ")
                        .append(arg.javaConverter)
                        .append("(\"")
                        .append(arg.name)
                        .append("\", ")
                        .append(arg.name)
                        .append(", ")
                        .append(arg.typeList())
                        .append(")\n");
            }

            if (!args.isEmpty())
                sb.append("\n");

            // function calls

            switch (functionCallType) {
                case SINGLETON:
                    generatePyFuncCallSingleton(sb, signatures, pyArgMap);
                    break;
                case SEQUENTIAL:
                    generatePyFuncCallSequential(sb, signatures, pyArgMap);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported FunctionCallType: " + functionCallType);
            }

            return sb.toString();
        }

        /**
         * Gets a list of functions with conflicting signatures.
         *
         * @param signatures java function signatures
         * @param pyArgMap possible python function arguments
         * @param argSet conflicting argument set
         * @return conflicting function names
         */
        private List<String> conflictingFuncs(final Map<Key, ArrayList<JavaFunction>> signatures,
                final Map<String, PyArg> pyArgMap, final Set<String> argSet) {
            return signatures.entrySet()
                    .stream()
                    .filter(e -> pyArgNames(e.getValue(), pyArgMap)
                            .stream()
                            .map(a -> new HashSet<>(Arrays.asList(a)))
                            .anyMatch(argSet::equals))
                    .map(e -> e.getKey().name)
                    .collect(Collectors.toList());
        }

        /**
         * Validate argument names. Make sure that code for a set of argument names has not yet been generated.
         *
         * @param argNames argument names
         * @param alreadyGenerated Set of argument names that have already been generated.
         * @param signatures java function signatures
         * @param pyArgMap possible python function arguments
         */
        private void validateArgNames(String[] argNames, Set<Set<String>> alreadyGenerated,
                Map<Key, ArrayList<JavaFunction>> signatures, Map<String, PyArg> pyArgMap) {
            final Set<String> argSet = new HashSet<>(Arrays.asList(argNames));

            if (alreadyGenerated.contains(argSet)) {
                throw new RuntimeException("Java functions have same signature: function=" + this + " sig="
                        + Arrays.toString(argNames) + " conflicts=" + conflictingFuncs(signatures, pyArgMap, argSet));
            } else {
                alreadyGenerated.add(argSet);
            }
        }

        /**
         * Generates code for calling a single java function from python.
         *
         * @param sb string builder
         * @param signatures java function signatures
         * @param pyArgMap possible python function arguments
         */
        private void generatePyFuncCallSingleton(final StringBuilder sb,
                final Map<Key, ArrayList<JavaFunction>> signatures, final Map<String, PyArg> pyArgMap) {
            final Set<Set<String>> alreadyGenerated = new HashSet<>();

            boolean isFirst = true;
            for (final Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
                final Key key = entry.getKey();
                final ArrayList<JavaFunction> sigs = entry.getValue();
                final List<String[]> argNameList = pyArgNames(sigs, pyArgMap);
                final List<String[]> nonNullableArgNameList = pyArgNames(sigs, pyArgMap, nullableParams);

                if (argNameList.size() != nonNullableArgNameList.size()) {
                    throw new RuntimeException(
                            "Full argument list size " + argNameList.size() + " and non-nullable list size "
                                    + nonNullableArgNameList.size() + " do not match for " + key);
                }

                for (int i = 0; i < nonNullableArgNameList.size(); i++) {
                    final String[] argNames = argNameList.get(i);
                    final String[] nonNullableArgNames = nonNullableArgNameList.get(i);
                    validateArgNames(argNames, alreadyGenerated, signatures, pyArgMap);
                    final String[] quotedNonNullableArgNames =
                            Arrays.stream(nonNullableArgNames).map(s -> "\"" + s + "\"").toArray(String[]::new);
                    final boolean hasNullables = nonNullableArgNames.length < argNames.length;

                    if (argNames.length == 0) {
                        sb.append(INDENT)
                                .append(INDENT)
                                .append(isFirst ? "if" : "elif")
                                .append(" not non_null_args:\n");
                    } else if (hasNullables) {
                        sb.append(INDENT)
                                .append(INDENT)
                                .append(isFirst ? "if" : "elif")
                                .append(" set({")
                                .append(String.join(", ", quotedNonNullableArgNames))
                                .append("}).issubset(non_null_args):\n");
                    } else {
                        sb.append(INDENT)
                                .append(INDENT)
                                .append(isFirst ? "if" : "elif")
                                .append(" non_null_args == {")
                                .append(String.join(", ", quotedNonNullableArgNames))
                                .append("}:\n");
                    }
                    sb.append(INDENT)
                            .append(INDENT)
                            .append(INDENT)
                            .append("return Figure(j_figure=self.j_figure.")
                            .append(key.name)
                            .append("(")
                            .append(String.join(", ", argNames))
                            .append("))\n");
                    isFirst = false;
                }
            }

            sb.append(INDENT)
                    .append(INDENT)
                    .append("else:\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append(INDENT)
                    .append("raise DHError(f\"unsupported parameter combination: {non_null_args}\")\n");
        }

        /**
         * Generates code for sequentially calling multiple java functions from python.
         *
         * @param sb string builder
         * @param signatures java function signatures
         * @param pyArgMap possible python function arguments
         */
        private void generatePyFuncCallSequential(final StringBuilder sb,
                final Map<Key, ArrayList<JavaFunction>> signatures, final Map<String, PyArg> pyArgMap) {
            sb.append(INDENT)
                    .append(INDENT)
                    .append("f_called = False\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append("j_figure = self.j_figure\n\n");

            boolean needsMskCheck = false;

            final Set<Set<String>> alreadyGenerated = new HashSet<>();

            final List<Pair<Key, String[]>> items = new ArrayList<>();

            for (final Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
                final Key key = entry.getKey();
                final ArrayList<JavaFunction> sigs = entry.getValue();
                final List<String[]> argNames = pyArgNames(sigs, pyArgMap);

                for (String[] argName : argNames) {
                    needsMskCheck = needsMskCheck || Arrays.stream(argName).anyMatch("multi_series_key"::equals);
                    final Pair<Key, String[]> e = new Pair<>(key, argName);
                    items.add(e);
                }
            }

            if (needsMskCheck) {
                sb.append(INDENT)
                        .append(INDENT)
                        .append("multi_series_key_used = False\n\n");
            }

            // sort from largest number of args to smallest number of args so that the most specific method is called
            items.sort((a, b) -> b.second.length - a.second.length);

            for (Pair<Key, String[]> item : items) {
                final Key key = item.first;
                final String[] an = item.second;

                final boolean mskUsed = Arrays.stream(an).anyMatch("multi_series_key"::equals);

                validateArgNames(an, alreadyGenerated, signatures, pyArgMap);
                final String[] quotedAn = Arrays.stream(an).map(s -> "\"" + s + "\"").toArray(String[]::new);

                // prevent removal of multi_series_key until after it's been fully used
                final String[] filteredQuotedAn = Arrays.stream(quotedAn)
                        .filter(s -> !s.equals("\"multi_series_key\""))
                        .toArray(String[]::new);

                if (quotedAn.length == 0) {
                    sb.append(INDENT)
                            .append(INDENT)
                            .append("if set()")
                            .append(".issubset(non_null_args):\n");
                } else {
                    sb.append(INDENT)
                            .append(INDENT)
                            .append("if {")
                            .append(String.join(", ", quotedAn))
                            .append("}.issubset(non_null_args):\n");
                }
                sb.append(INDENT)
                        .append(INDENT)
                        .append(INDENT)
                        .append("j_figure = j_figure.")
                        .append(key.name)
                        .append("(")
                        .append(String.join(", ", an))
                        .append(")\n")
                        .append(INDENT)
                        .append(INDENT)
                        .append(INDENT)
                        .append("non_null_args = non_null_args.difference({")
                        .append(String.join(", ", filteredQuotedAn))
                        .append("})\n")
                        .append(INDENT)
                        .append(INDENT)
                        .append(INDENT)
                        .append("f_called = True\n");

                if (mskUsed) {
                    sb.append(INDENT)
                            .append(INDENT)
                            .append(INDENT)
                            .append("multi_series_key_used = True\n");
                }

                sb.append("\n");
            }

            if (needsMskCheck) {
                sb.append(INDENT)
                        .append(INDENT)
                        .append("if multi_series_key_used:\n")
                        .append(INDENT)
                        .append(INDENT)
                        .append(INDENT)
                        .append("non_null_args = non_null_args.difference({\"multi_series_key\"})\n\n");
            }

            sb.append(INDENT)
                    .append(INDENT)
                    .append("if not f_called or non_null_args:\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append(INDENT)
                    .append("raise DHError(f\"unsupported parameter combination: {non_null_args}\")\n\n");

            sb.append(INDENT)
                    .append(INDENT)
                    .append("return Figure(j_figure=j_figure)\n");
        }

        /**
         * Generates code for the python function body. If this method is set to not generate, an empty string is
         * returned.
         *
         * @param signatures java functions
         * @return code for the python function body.
         */
        public String generatePy(final Map<Key, ArrayList<JavaFunction>> signatures) {
            if (!generate) {
                return "";
            }

            final List<PyArg> args = pyArgs(signatures);
            final String sig = generatePyFuncDef(args);
            final String pydocs = generatePyDocString(args);
            final String pybody = generatePyFuncBody(args, signatures);
            return sig + pydocs + pybody;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    /**
     * Generates python code for the module.
     * <p>
     * NOTE: when python code is generated, the relevant methods are removed from signatures.
     *
     * @param signatures java method signatures
     * @return generated python code
     * @throws IOException problem reading the preamble
     */
    private static String generatePy(final Map<Key, ArrayList<JavaFunction>> signatures) throws IOException {

        final StringBuilder sb = new StringBuilder();

        final String preamble = Files.readString(Path.of(figureWrapperPreamble));
        sb.append(preamble);
        sb.append("\n");

        final List<PyFunc> pyFuncs = getPyFuncs();

        for (final PyFunc pyFunc : pyFuncs) {
            final Map<Key, ArrayList<JavaFunction>> sigs = pyFunc.getSignatures(signatures);

            sigs.forEach((k, v) -> signatures.remove(k));

            final String pyFuncCode = pyFunc.generatePy(sigs);
            sb.append("\n").append(pyFuncCode);
        }

        return sb.toString();
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * A map of Java parameter names to Python arguments.
     *
     * @return map of Java parameter names to Python arguments.
     */
    private static Map<String, PyArg> getPyArgs() {
        final Map<String, PyArg> rst = new TreeMap<>();

        final String[] taStr = new String[] {"str"};
        final String[] taStrs = new String[] {"List[str]"};
        final String[] taBool = new String[] {"bool"};
        final String[] taInt = new String[] {"int"};
        final String[] taFloat = new String[] {"float"};
        final String[] taFloats = new String[] {"List[float]"};
        final String[] taFloatStr = new String[] {"str", "float"};
        final String[] taCallable = new String[] {"Callable"};
        final String[] taTable = new String[] {"Table", "SelectableDataSet"};

        final String[] taDataCategory = new String[] {"str", "List[str]", "List[int]", "List[float]"};
        final String[] taDataNumeric = new String[] {"str", "List[int]", "List[float]", "List[Instant]"};
        final String[] taDataTime = new String[] {"str", "List[Instant]"};
        final String[] taMultiSeriesKey = new String[] {"List[Any]"}; // todo keys are technically Object[]. How to
        // support?
        final String[] taColor = new String[] {"str", "int", "Color"};
        final String[] taAxisFormat = new String[] {"AxisFormat"};
        final String[] taAxisTransform = new String[] {"AxisTransform"};
        final String[] taFont = new String[] {"Font"};
        final String[] taBusinessCalendar = new String[] {"str", "BusinessCalendar"};
        final String[] taPlotStyle = new String[] {"str", "PlotStyle"};
        final String[] taLineStyle = new String[] {"str", "LineStyle"};
        final String[] taPointColors = new String[] {"str", "int", "Color", "List[str]", "List[int]", "List[Color]",
                "Callable", "Dict[Any,str]", "Dict[Any,int]", "Dict[Any,Color]"};
        final String[] taPointLabels = new String[] {"str", "List[str]", "Callable", "Dict[Any,str]"};
        final String[] taPointShapes = new String[] {"str", "Shape", "List[str]", "List[Shape]", "Callable",
                "Dict[Any,str]", "Dict[Any,Shape]"};
        final String[] taPointSizes = new String[] {"int", "float", "List[int]", "List[float]", "Callable",
                "Dict[Any,int]", "Dict[Any,float]"};

        ////////////////////////////////////////////////////////////////

        rst.put("name", new PyArg(1, "name", taStr, "name", null));
        rst.put("dim", new PyArg(2, "dim", taInt, "dimension of the axis", null));
        rst.put("id", new PyArg(3, "axes", taInt, "identifier", null));
        rst.put("group", new PyArg(4, "group", taInt, "group for the data series.", null));
        rst.put("seriesName", new PyArg(5, "series_name", taStr, "name of the data series", null));
        rst.put("t", new PyArg(6, "t", taTable, "table or selectable data set (e.g. OneClick filterable table)", null));
        rst.put("sds", rst.get("t"));
        rst.put("categories", new PyArg(7, "category", taDataCategory, "discrete data or column name", null));
        rst.put("category", rst.get("categories"));
        rst.put("x", new PyArg(8, "x", taDataNumeric, "x-values or column name", null));
        rst.put("time", new PyArg(9, "x", taDataTime, "x-values or column name", null));
        rst.put("xLow", new PyArg(10, "x_low", taDataNumeric, "lower x error bar", null));
        rst.put("xHigh", new PyArg(11, "x_high", taDataNumeric, "upper x error bar", null));
        rst.put("y", new PyArg(12, "y", taDataNumeric, "y-values or column name", null));
        rst.put("yLow", new PyArg(13, "y_low", taDataNumeric, "lower y error bar", null));
        rst.put("yHigh", new PyArg(14, "y_high", taDataNumeric, "upper y error bar", null));
        rst.put("open", new PyArg(15, "open", taDataNumeric, "bar open y-values.", null));
        rst.put("high", new PyArg(16, "high", taDataNumeric, "bar high y-values.", null));
        rst.put("low", new PyArg(17, "low", taDataNumeric, "bar low y-values.", null));
        rst.put("close", new PyArg(18, "close", taDataNumeric, "bar close y-values.", null));
        rst.put("function", new PyArg(19, "function", taCallable, "function", null));
        rst.put("xmin", new PyArg(20, "xmin", taFloat, "minimum x value to display", null));
        rst.put("xmax", new PyArg(21, "xmax", taFloat, "maximum x value to display", null));
        rst.put("nbins", new PyArg(22, "nbins", taInt, "number of bins", null));
        rst.put("multiSeriesKey", new PyArg(23, "multi_series_key", taMultiSeriesKey,
                "multi-series keys or a column name containing keys.", null));
        rst.put("byColumns", new PyArg(24, "by", taStrs, "columns that hold grouping data", "_no_convert_j"));
        rst.put("hasXTimeAxis", new PyArg(25, "x_time_axis", taBool, "whether to treat the x-values as times", null));
        rst.put("hasYTimeAxis", new PyArg(26, "y_time_axis", taBool, "whether to treat the y-values as times", null));
        rst.put("path", new PyArg(27, "path", taStr, "output path.", null));
        rst.put("height", new PyArg(28, "height", taInt, "figure height.", null));
        rst.put("width", new PyArg(29, "width", taInt, "figure width.", null));
        rst.put("wait",
                new PyArg(30, "wait", taBool, "whether to hold the calling thread until the file is written.", null));
        rst.put("timeoutSeconds", new PyArg(31, "timeout_seconds", taInt,
                "timeout in seconds to wait for the file to be written.", null));
        rst.put("removeSeriesNames", new PyArg(32, "remove_series", taStrs, "names of series to remove", null));
        rst.put("removeChartIndex", new PyArg(33, "remove_chart_index", taInt,
                "index from the Figure's grid to remove. The index starts at 0 in the upper left hand corner of the grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1][2, 3].",
                null));
        rst.put("removeChartRowNum", new PyArg(34, "remove_chart_row", taInt,
                "row index in this Figure's grid. The row index starts at 0.", null));
        rst.put("removeChartColNum", new PyArg(35, "remove_chart_col", taInt,
                "column index in this Figure's grid. The row index starts at 0.", null));
        rst.put("updateIntervalMillis",
                new PyArg(36, "update_millis", taInt, "update interval in milliseconds.", null));
        rst.put("index", new PyArg(37, "index", taInt,
                "index from the Figure's grid. The index starts at 0 in the upper left hand corner of the grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1] [2, 3].",
                null));
        rst.put("rowNum",
                new PyArg(38, "row", taInt, "row index in the Figure's grid. The row index starts at 0.", null));
        rst.put("colNum",
                new PyArg(39, "col", taInt, "column index in this Figure's grid. The column index starts at 0.", null));
        rst.put("rowSpan", new PyArg(40, "row_span", taInt, "how many rows high.", null));
        rst.put("colSpan", new PyArg(41, "col_span", taInt, "how many rows wide.", null));
        rst.put("orientation", new PyArg(42, "orientation", taStr, "plot orientation.", null));
        rst.put("gridVisible", new PyArg(43, "grid_visible", taBool, "x-grid and y-grid are visible.", null));
        rst.put("xGridVisible", new PyArg(44, "x_grid_visible", taBool, "x-grid is visible.", null));
        rst.put("yGridVisible", new PyArg(45, "y_grid_visible", taBool, "y-grid is visible.", null));
        rst.put("pieLabelFormat",
                new PyArg(46, "pie_label_format", taStr, "pie chart format of the percentage point label.", null));
        rst.put("title", new PyArg(47, "title", taStr, "title", null));
        rst.put("titleColumns", new PyArg(48, "columns", taStrs, "columns to include in the title", null));
        rst.put("titleFormat", new PyArg(49, "format", taStr,
                "a java.text.MessageFormat format string for formatting column values in the title", null));
        rst.put("maxTitleRows",
                new PyArg(50, "max_rows", taInt, "maximum number of row values to show in title", null));
        rst.put("showColumnNamesInTitle", new PyArg(51, "column_names_in_title", taBool,
                "whether to show column names in title. If this is true, the title format will include the column name before the comma separated values; otherwise only the comma separated values will be included.",
                null));
        rst.put("label", new PyArg(52, "label", taStr, "label", null));
        rst.put("color", new PyArg(53, "color", taColor, "color", null));
        rst.put("font", new PyArg(54, "font", taFont, "font", null));
        rst.put("axisFormat", new PyArg(58, "format", taAxisFormat, "label format", null));
        rst.put("axisFormatPattern", new PyArg(59, "format_pattern", taStr, "label format pattern", null));
        rst.put("plotStyle", new PyArg(60, "plot_style", taPlotStyle, "plot style", null));
        rst.put("min", new PyArg(61, "min", taFloatStr, "minimum value to display", null));
        rst.put("max", new PyArg(62, "max", taFloatStr, "maximum value to display", null));
        rst.put("npoints", new PyArg(63, "npoints", taInt, "number of points", null));
        rst.put("invert", new PyArg(64, "invert", taBool, "invert the axis.", null));
        rst.put("useLog", new PyArg(65, "log", taBool, "log axis", null));
        rst.put("useBusinessTime",
                new PyArg(66, "business_time", taBool, "business time axis using the default calendar", null));
        rst.put("calendar",
                new PyArg(67, "calendar", taBusinessCalendar, "business time axis using the specified calendar", null));
        rst.put("transform", new PyArg(68, "transform", taAxisTransform, "axis transform.", null));
        rst.put("gapBetweenTicks", new PyArg(69, "gap", taFloat, "distance between ticks.", null));
        rst.put("tickLocations", new PyArg(70, "loc", taFloats, "coordinates of the tick locations.", null));
        rst.put("nminor",
                new PyArg(71, "nminor", taInt, "number of minor ticks between consecutive major ticks.", null));
        rst.put("angle", new PyArg(72, "angle", taInt, "angle in degrees.", null));
        rst.put("toolTipPattern", new PyArg(73, "tool_tip_pattern", taStr, "x and y tool tip format pattern", null));
        rst.put("xToolTipPattern", new PyArg(74, "x_tool_tip_pattern", taStr, "x tool tip format pattern", null));
        rst.put("yToolTipPattern", new PyArg(75, "y_tool_tip_pattern", taStr, "y tool tip format pattern", null));
        rst.put("errorBarColor", new PyArg(76, "error_bar_color", taColor, "error bar color.", null));
        rst.put("gradientVisible", new PyArg(77, "gradient_visible", taBool, "bar gradient visibility.", null));
        rst.put("namingFunction", new PyArg(78, "naming_function", taCallable, "series naming function", null));
        rst.put("lineStyle", new PyArg(79, "style", taLineStyle, "line style", null));
        rst.put("pointColor", new PyArg(80, "color", taPointColors, "colors or a column name containing colors", null));
        rst.put("pointColors", rst.get("pointColor"));
        rst.put("pointLabel", new PyArg(81, "label", taPointLabels, "labels or a column name containing labels", null));
        rst.put("pointLabels", rst.get("pointLabel"));
        rst.put("pointShape", new PyArg(82, "shape", taPointShapes, "shapes or a column name containing shapes", null));
        rst.put("pointShapes", rst.get("pointShape"));
        rst.put("pointSize", new PyArg(83, "size", taPointSizes, "sizes or a column name containing sizes", null));
        rst.put("pointSizes", rst.get("pointSize"));
        rst.put("pointLabelFormat", new PyArg(84, "label_format", taStr, "point label format.", null));
        rst.put("visible", new PyArg(85, "visible", taInt, "true to draw the design element; false otherwise.", null));
        rst.put("ids", new PyArg(86, "id", taStr, "column name containing IDs", null));
        rst.put("parents", new PyArg(87, "parent", taStr, "column name containing parent IDs", null));
        rst.put("values", new PyArg(88, "value", taStr, "column name containing values", null));
        rst.put("labels", new PyArg(89, "label", taStr, "column name containing labels", null));
        rst.put("hoverTexts", new PyArg(90, "hover_text", taStr, "column name containing hover text", null));
        rst.put("colors", new PyArg(91, "color", taStr, "column name containing color", null));

        ////////////////////////////////////////////////////////////////

        return rst;
    }

    /**
     * Supported Python functions to generate.
     *
     * @return supported Python functions to generate.
     */
    private static List<PyFunc> getPyFuncs() {
        final ArrayList<PyFunc> rst = new ArrayList<>();

        final FunctionCallType SINGLETON = FunctionCallType.SINGLETON;
        final FunctionCallType SEQUENTIAL = FunctionCallType.SEQUENTIAL;

        ////////////////////////////////////////////////////////////////

        rst.add(new PyFunc("show", SINGLETON, new String[] {"show"}, null,
                "Creates a displayable version of the figure and returns it."));
        rst.add(new PyFunc("save", SINGLETON, new String[] {"save"}, new String[] {"path"},
                "Saves the Figure as an image."));

        rst.add(new PyFunc("figure", SEQUENTIAL, new String[] {"figureRemoveSeries", "removeChart", "updateInterval"},
                null,
                "Updates the figure's configuration."));
        rst.add(new PyFunc("figure_title", SEQUENTIAL,
                new String[] {"figureTitle", "figureTitleColor", "figureTitleFont"}, null,
                "Sets the title of the figure."));
        rst.add(new PyFunc("new_chart", SINGLETON, new String[] {"newChart"}, null,
                "Adds a new chart to this figure."));
        rst.add(new PyFunc("chart", SEQUENTIAL,
                new String[] {"chart", "chartRemoveSeries", "span", "rowSpan", "colSpan", "plotOrientation",
                        "gridLinesVisible", "xGridLinesVisible", "yGridLinesVisible", "piePercentLabelFormat"},
                null,
                "Gets a chart from the figure's grid and updates a chart's configuration."));
        rst.add(new PyFunc("chart_title", SEQUENTIAL,
                new String[] {"chartTitle", "chartTitleColor", "chartTitleFont", "maxRowsInTitle"}, null,
                "Sets the title of the chart."));
        rst.add(new PyFunc("chart_legend", SEQUENTIAL, new String[] {"legendColor", "legendFont", "legendVisible"},
                null,
                "Updates a chart's legend's configuration."));
        rst.add(new PyFunc("new_axes", SINGLETON, new String[] {"newAxes"}, null,
                "Creates new axes."));
        rst.add(new PyFunc("axes", SEQUENTIAL, new String[] {"axes", "axesRemoveSeries", "plotStyle"}, null,
                "Gets specific axes from the chart and updates the chart's axes's configurations."));
        rst.add(new PyFunc(
                "axis", SEQUENTIAL, new String[] {"axis", "axisColor", "axisFormat", "axisFormatPattern", "axisLabel",
                        "axisLabelFont", "invert", "log", "min", "max", "range", "businessTime", "transform"},
                null,
                "Gets a specific axis from a chart's axes and updates the axis's configurations."));
        rst.add(new PyFunc("ticks", SEQUENTIAL, new String[] {"ticks", "ticksFont", "ticksVisible", "tickLabelAngle"},
                null,
                "Updates the configuration for major ticks of an axis."));
        rst.add(new PyFunc("ticks_minor", SEQUENTIAL, new String[] {"minorTicks", "minorTicksVisible"}, null,
                "Updates the configuration for minor ticks of an axis."));
        rst.add(new PyFunc("twin", SINGLETON, new String[] {"twin"}, null,
                "Creates a new Axes which shares one Axis with the current Axes. For example, this is used for creating plots with a common x-axis but two different y-axes."));
        rst.add(new PyFunc(
                "x_axis", SEQUENTIAL, new String[] {"xAxis", "xColor", "xFormat", "xFormatPattern", "xLabel",
                        "xLabelFont", "xInvert", "xLog", "xMin", "xMax", "xRange", "xBusinessTime", "xTransform"},
                null,
                "Gets the x-Axis from a chart's axes and updates the x-Axis's configurations."));
        rst.add(new PyFunc("x_ticks", SEQUENTIAL,
                new String[] {"xTicks", "xTicksFont", "xTicksVisible", "xTickLabelAngle"}, null,
                "Updates the configuration for major ticks of the x-Axis."));
        rst.add(new PyFunc("x_ticks_minor", SEQUENTIAL, new String[] {"xMinorTicks", "xMinorTicksVisible"}, null,
                "Updates the configuration for minor ticks of the x-Axis."));
        rst.add(new PyFunc("x_twin", SINGLETON, new String[] {"twinX"}, null,
                "Creates a new Axes which shares the x-Axis with the current Axes. For example, this is used for creating plots with a common x-axis but two different y-axes."));
        rst.add(new PyFunc(
                "y_axis", SEQUENTIAL, new String[] {"yAxis", "yColor", "yFormat", "yFormatPattern", "yLabel",
                        "yLabelFont", "yInvert", "yLog", "yMin", "yMax", "yRange", "yBusinessTime", "yTransform"},
                null,
                "Gets the y-Axis from a chart's axes and updates the y-Axis's configurations."));
        rst.add(new PyFunc("y_ticks", SEQUENTIAL,
                new String[] {"yTicks", "yTicksFont", "yTicksVisible", "yTickLabelAngle"}, null,
                "Updates the configuration for major ticks of the y-Axis."));
        rst.add(new PyFunc("y_ticks_minor", SEQUENTIAL, new String[] {"yMinorTicks", "yMinorTicksVisible"}, null,
                "Updates the configuration for minor ticks of the y-Axis."));
        rst.add(new PyFunc("y_twin", SINGLETON, new String[] {"twinY"}, null,
                "Creates a new Axes which shares the y-Axis with the current Axes. For example, this is used for creating plots with a common y-axis but two different x-axes."));
        rst.add(new PyFunc("series", SEQUENTIAL, new String[] {"series", "group", "seriesColor", "toolTipPattern",
                "xToolTipPattern", "yToolTipPattern", "errorBarColor", "gradientVisible", "seriesNamingFunction"}, null,
                "Gets a specific data series and updates the data series's configurations."));
        rst.add(new PyFunc("point", SEQUENTIAL, new String[] {"pointColor", "pointLabel", "pointLabelFormat",
                "pointShape", "pointSize", "pointsVisible"}, null,
                "Sets the point color, label, size, visibility, etc."));
        rst.add(new PyFunc("line", SEQUENTIAL, new String[] {"lineColor", "lineStyle", "linesVisible"}, null,
                "Sets the line color, style, visibility."));
        rst.add(new PyFunc("func", SEQUENTIAL, new String[] {"funcNPoints", "funcRange"}, null,
                "Updates the configuration for plotting a function."));
        rst.add(new PyFunc("plot_xy", SINGLETON, new String[] {"plot", "plotBy", "errorBarX", "errorBarXBy",
                "errorBarY", "errorBarYBy", "errorBarXY", "errorBarXYBy"}, new String[] {"series_name"},
                "Creates an XY plot."));
        rst.add(new PyFunc("plot_xy_hist", SINGLETON, new String[] {"histPlot"}, new String[] {"series_name"},
                "Creates an XY histogram."));
        rst.add(new PyFunc("plot_cat", SINGLETON, new String[] {"catPlot", "catPlotBy", "catErrorBar", "catErrorBarBy"},
                new String[] {"series_name"},
                "Creates a plot with a discrete, categorical axis. Categorical data must not have duplicates."));
        rst.add(new PyFunc("plot_cat_hist", SINGLETON, new String[] {"catHistPlot"}, new String[] {"series_name"},
                "Creates a histogram with a discrete axis. Charts the frequency of each unique element in the input data."));
        rst.add(new PyFunc("plot_pie", SINGLETON, new String[] {"piePlot"}, new String[] {"series_name"},
                "Creates a pie plot. Categorical data must not have duplicates."));
        rst.add(new PyFunc("plot_ohlc", SINGLETON, new String[] {"ohlcPlot", "ohlcPlotBy"},
                new String[] {"series_name"},
                "Creates an open-high-low-close plot."));
        rst.add(new PyFunc("plot_treemap", SINGLETON, new String[] {"treemapPlot"},
                new String[] {"series_name", "t", "id", "parent"},
                new String[] {"value", "label", "hover_text", "color"},
                "Creates a treemap. Must have only one root."));

        ////////////////////////////////////////////////////////////////

        // Don't think that pointColorInteger gets used, so python code is not generated
        rst.add(new PyFunc("point_color_integer", SINGLETON, new String[] {"pointColorInteger"},
                new String[] {"colors"}, "TODO pydoc", false));

        Collections.sort(rst);
        return rst;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static void printMethodSignatures(final Map<Key, ArrayList<JavaFunction>> signatures) {
        for (final Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
            final Key key = entry.getKey();
            final ArrayList<JavaFunction> sigs = entry.getValue();
            System.out.println(describeMethodSignatures(key, sigs));
        }
    }

}
