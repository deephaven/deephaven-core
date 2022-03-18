package io.deephaven.plot.util;

import io.deephaven.libs.GroovyStaticImportGenerator.JavaFunction;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Generate the plotting Figure API for Python V2.
 */
public class GeneratePyV2FigureAPI {

    private static final String INDENT = "    ";
    private static final String JCLASS = "io.deephaven.plot.Figure";
    private static final String PREAMBLE = "Generators/src/main/java/io/deephaven/pythonPreambles/plotV2.py";

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Java method signatures that are excluded from analysis.
     * <p>
     * Key = java function name
     * Value = array of java function signatures to exclude.  The signatures are lists of parameter names.  The order matters.
     */
    private static final Map<String, String[][]> javaExcludes = new HashMap<>() {{
        put("invert", new String[][]{new String[]{}});
        put("log", new String[][]{new String[]{}});
        put("businessTime", new String[][]{new String[]{}});
        put("xInvert", new String[][]{new String[]{}});
        put("xLog", new String[][]{new String[]{}});
        put("xBusinessTime", new String[][]{new String[]{}});
        put("yInvert", new String[][]{new String[]{}});
        put("yLog", new String[][]{new String[]{}});
        put("yBusinessTime", new String[][]{new String[]{}});
    }};

    /**
     * Gets the method signatures of public JCLASS methods.
     *
     * @return Map of method keys to a list of all relevant method signatures.
     * @throws ClassNotFoundException JCLASS is not found
     */
    public static Map<Key, ArrayList<JavaFunction>> getMethodSignatures() throws ClassNotFoundException {
        final Class<?> c = Class.forName(JCLASS);
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
     * @param key        signature key
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
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

        public PyArg(final int precedence, final String name, final String[] typeAnnotations, final String docString, final String javaConverter) {
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
            return typeAnnotations.length == 1 ? typeAnnotations[0] : "Union[" + String.join(",", typeAnnotations) + "]";
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
        private final String pydoc;
        private final boolean generate;

        public PyFunc(final String name, final FunctionCallType functionCallType, final String[] javaFuncs, final String[] requiredParams, final String pydoc, final boolean generate) {
            this.name = name;
            this.functionCallType = functionCallType;
            this.javaFuncs = javaFuncs;
            this.requiredParams = requiredParams == null ? new String[]{} : requiredParams;
            this.pydoc = pydoc;
            this.generate = generate;
        }

        public PyFunc(final String name, final FunctionCallType functionCallType, final String[] javaFuncs, final String[] requiredParams, final String pydoc) {
            this(name, functionCallType, javaFuncs, requiredParams, pydoc, true);
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

            final Map<Key, ArrayList<JavaFunction>> rst = new TreeMap<>(Comparator.comparingInt(a -> order.get(a.name)));

            rst.putAll(
                    signatures
                            .entrySet()
                            .stream()
                            .filter(e -> order.containsKey(e.getKey().name))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );

            if (rst.size() != javaFuncs.length) {
                final Set<String> required = new TreeSet<>(Arrays.asList(javaFuncs));
                final Set<String> actual = rst.keySet().stream().map(k -> k.name).collect(Collectors.toSet());
                required.removeAll(actual);
                throw new IllegalArgumentException("Java methods are not present in signatures: func=" + this + " methods=" + required);
            }

            return rst;
        }

        /**
         * Is the parameter required for the function?
         *
         * @param parameter python parameter
         * @return is the parameter requried for the function?
         */
        public boolean isRequired(final PyArg parameter) {
            return Arrays.asList(requiredParams).contains(parameter.name);
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
         * @return valid Java method argument name combinations.
         */
        private static List<String[]> pyArgNames(final ArrayList<JavaFunction> signatures, final Map<String, PyArg> pyArgMap) {
            final Set<Set<String>> seen = new HashSet<>();

            return javaArgNames(signatures)
                    .stream()
                    .map(an -> Arrays.stream(an).map(s -> pyArgMap.get(s).name).toArray(String[]::new))
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
                            throw new IllegalArgumentException("Unsupported python parameter: func=" + this + " param=" + param);
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
         * @param args       function arguments
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

            sb.append("\n");

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
         * @param pyArgMap   possible python function arguments
         * @param argSet     conflicting argument set
         * @return conflicting function names
         */
        private List<String> conflictingFuncs(final Map<Key, ArrayList<JavaFunction>> signatures, final Map<String, PyArg> pyArgMap, final Set<String> argSet) {
            return signatures.entrySet()
                    .stream()
                    .filter(e ->
                            pyArgNames(e.getValue(), pyArgMap)
                                    .stream()
                                    .map(a -> new HashSet<>(Arrays.asList(a)))
                                    .anyMatch(argSet::equals)
                    )
                    .map(e -> e.getKey().name)
                    .collect(Collectors.toList());
        }

        /**
         * Validate argument names.  Make sure that code for a set of argument names has not yet been generated.
         *
         * @param argNames         argument names
         * @param alreadyGenerated Set of argument names that have already been generated.
         * @param signatures       java function signatures
         * @param pyArgMap         possible python function arguments
         */
        private void validateArgNames(String[] argNames, Set<Set<String>> alreadyGenerated, Map<Key, ArrayList<JavaFunction>> signatures, Map<String, PyArg> pyArgMap) {
            final Set<String> argSet = new HashSet<>(Arrays.asList(argNames));

            if (alreadyGenerated.contains(argSet)) {
                throw new RuntimeException("Java functions have same signature: function=" + this + " sig=" + Arrays.toString(argNames) + " conflicts=" + conflictingFuncs(signatures, pyArgMap, argSet));
            } else {
                alreadyGenerated.add(argSet);
            }
        }

        /**
         * Generates code for calling a single java function from python.
         *
         * @param sb         string builder
         * @param signatures java function signatures
         * @param pyArgMap   possible python function arguments
         */
        private void generatePyFuncCallSingleton(final StringBuilder sb, final Map<Key, ArrayList<JavaFunction>> signatures, final Map<String, PyArg> pyArgMap) {
            final Set<Set<String>> alreadyGenerated = new HashSet<>();

            for (final Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
                final Key key = entry.getKey();
                final ArrayList<JavaFunction> sigs = entry.getValue();
                final List<String[]> argNames = pyArgNames(sigs, pyArgMap);

                boolean isFirst = true;

                for (final String[] an : argNames) {
                    validateArgNames(an, alreadyGenerated, signatures, pyArgMap);
                    final String[] quoted_an = Arrays.stream(an).map(s -> "\"" + s + "\"").toArray(String[]::new);

                    sb.append(INDENT)
                            .append(INDENT)
                            .append(isFirst ? "if" : "elif")
                            .append(" non_null_args == {")
                            .append(String.join(", ", quoted_an))
                            .append("}:\n")
                            .append(INDENT)
                            .append(INDENT)
                            .append(INDENT)
                            .append("return Figure(self.j_figure.")
                            .append(key.name)
                            .append("(")
                            .append(String.join(", ", an))
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
         * @param sb         string builder
         * @param signatures java function signatures
         * @param pyArgMap   possible python function arguments
         */
        private void generatePyFuncCallSequential(final StringBuilder sb, final Map<Key, ArrayList<JavaFunction>> signatures, final Map<String, PyArg> pyArgMap) {

            sb.append(INDENT)
                    .append(INDENT)
                    .append("f_called = False\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append("j_figure = self.j_figure\n\n");


            final Set<Set<String>> alreadyGenerated = new HashSet<>();

            for (final Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
                final Key key = entry.getKey();
                final ArrayList<JavaFunction> sigs = entry.getValue();
                final List<String[]> argNames = pyArgNames(sigs, pyArgMap);

                for (final String[] an : argNames) {
                    validateArgNames(an, alreadyGenerated, signatures, pyArgMap);
                    final String[] quoted_an = Arrays.stream(an).map(s -> "\"" + s + "\"").toArray(String[]::new);

                    sb.append(INDENT)
                            .append(INDENT)
                            .append("if {")
                            .append(String.join(", ", quoted_an))
                            .append("}.issubset(non_null_args):\n")
                            .append(INDENT)
                            .append(INDENT)
                            .append(INDENT)
                            .append("j_figure = self.j_figure.")
                            .append(key.name)
                            .append("(")
                            .append(String.join(", ", an))
                            .append(")\n")
                            .append(INDENT)
                            .append(INDENT)
                            .append(INDENT)
                            .append("non_null_args = non_null_args.difference({")
                            .append(String.join(", ", quoted_an))
                            .append("})\n")
                            .append(INDENT)
                            .append(INDENT)
                            .append(INDENT)
                            .append("f_called = True\n\n");
                }
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
                    .append("return Figure(j_figure)\n");
        }

        /**
         * Generates code for the python function body.  If this method is set to not generate, an empty string is returned.
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

        final String preamble = Files.readString(Path.of(PREAMBLE));
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

        //todo check type annotations

        final String[] taStr = new String[]{"str"};
        final String[] taStrs = new String[]{"List[str]"};
        final String[] taBool = new String[]{"bool"};
        final String[] taInt = new String[]{"int"};
        final String[] taInts = new String[]{"List[int]"};
        final String[] taFloat = new String[]{"float"};
        final String[] taFloats = new String[]{"List[float]"};
        final String[] taCallable = new String[]{"Callable"};
        final String[] taTable = new String[]{"Table", "SelectableDataSet"};
        final String[] taDataCategory = new String[]{"str", "List[str]", "List[int]", "List[float]"};
        final String[] taDataNumeric = new String[]{"str", "List[int]", "List[float]", "List[DateTime]"}; //todo support numpy, //todo support other datetimes
        final String[] taDataTime = new String[]{"str", "List[DateTime]"}; //todo support numpy, //todo support other datetimes

        final String[] taKey = new String[]{"List[Any]"}; //todo keys are technically Object[].  How to support?
        final String[] taColor = new String[]{"str", "int", "Color"}; //todo support Color (io.deephaven.gui.color.Paint)
        final String[] taColors = new String[]{"str", "List[str]", "List[Color]", "List[int]", "Dict[str,Color]", "Callable"}; //todo support Color (io.deephaven.gui.color.Paint)
        final String[] taShape = new String[]{"str", "Shape"}; //todo support Shape (io.deephaven.gui.shape.Shape)
        final String[] taShapes = new String[]{"List[str]", "List[Shape]", "Dict[str,String]", "Callable"}; //todo support Shape (io.deephaven.gui.shape.Shape)
        final String[] taAxisFormat = new String[]{"AxisFormat"}; //todo support io.deephaven.plot.axisformatters.AxisFormat
        final String[] taAxisTransform = new String[]{"AxisTransform"}; //todo support io.deephaven.plot.axistransformations.AxisTransform
        final String[] taFont = new String[]{"Font"}; //todo support io.deephaven.plot.Font
        final String[] taBusinessCalendar = new String[]{"BusinessCalendar"}; //todo support io.deephaven.time.calendar.BusinessCalendar
        final String[] taFactor = new String[]{"str", "int", "float"};
        final String[] taFactors = new String[]{"List[int]", "List[float]", "Dict[str,int]", "Dict[str,float]", "Callable"};

        ////////////////////////////////////////////////////////////////

        rst.put("seriesName", new PyArg(1, "series_name", taStr, "name of the data series", null));
        rst.put("t", new PyArg(2, "t", taTable, "table or selectable data set (e.g. OneClick filterable table)", null));
        rst.put("categories", new PyArg(3, "categories", taDataCategory, "discrete data or column name", null));
        rst.put("x", new PyArg(3, "x", taDataNumeric, "x-values or column name", null));
        rst.put("xLow", new PyArg(4, "x_low", taDataNumeric, "lower x error bar", null));
        rst.put("xHigh", new PyArg(5, "x_high", taDataNumeric, "upper x error bar", null));
        rst.put("y", new PyArg(6, "y", taDataNumeric, "y-values or column name", null));
        rst.put("yLow", new PyArg(7, "y_low", taDataNumeric, "lower y error bar", null));
        rst.put("yHigh", new PyArg(8, "y_high", taDataNumeric, "upper y error bar", null));
        rst.put("function", new PyArg(9, "function", taCallable, "function", null));
        rst.put("xmin", new PyArg(10, "xmin", taFloat, "minimum x value to display", null));
        rst.put("xmax", new PyArg(11, "xmax", taFloat, "maximum x value to display", null));
        rst.put("nbins", new PyArg(12, "nbins", taInt, "number of bins", null));
        rst.put("byColumns", new PyArg(13, "by", taStrs, "columns that hold grouping data", null));
        rst.put("hasXTimeAxis", new PyArg(14, "x_time_axis", taBool, "whether to treat the x-values as times", null));
        rst.put("hasYTimeAxis", new PyArg(15, "y_time_axis", taBool, "whether to treat the y-values as times", null));

        rst.put("path", new PyArg(1, "path", taStr, "output path.", null));
        rst.put("height", new PyArg(2, "height", taInt, "figure height.", null));
        rst.put("width", new PyArg(3, "width", taInt, "figure width.", null));
        rst.put("wait", new PyArg(4, "wait", taBool, "whether to hold the calling thread until the file is written.", null));
        rst.put("timeoutSeconds", new PyArg(5, "timeout_seconds", taInt, "timeout in seconds to wait for the file to be written.", null));

        rst.put("gridVisible", new PyArg(10, "grid_visible", taBool, "x-grid and y-grid are visible.", null));
        rst.put("xGridVisible", new PyArg(11, "x_grid_visible", taBool, "x-grid is visible.", null));
        rst.put("yGridVisible", new PyArg(12, "y_grid_visible", taBool, "y-grid is visible.", null));
        rst.put("pieLabelFormat", new PyArg(13, "pie_label_format", taStr, "pie chart format of the percentage point label.", null));

        rst.put("toolTipPattern", new PyArg(10, "tool_tip_pattern", taStr, "x and y tool tip format pattern", null));
        rst.put("xToolTipPattern", new PyArg(11, "x_tool_tip_pattern", taStr, "x tool tip format pattern", null));
        rst.put("yToolTipPattern", new PyArg(12, "y_tool_tip_pattern", taStr, "y tool tip format pattern", null));
        rst.put("errorBarColor", new PyArg(13, "error_bar_color", taColor, "error bar color.", null));
        rst.put("gradientVisible", new PyArg(14, "gradient_visible", taBool, "bar gradient visibility.", null));
        rst.put("namingFunction", new PyArg(15, "naming_function", taCallable, "series naming function", null));

        rst.put("removeSeriesNames", new PyArg(10, "remove_series", taStrs, "names of series to remove", null));

        rst.put("removeChartIndex", new PyArg(10, "remove_chart_index", taInt, "index from the Figure's grid to remove. The index starts at 0 in the upper left hand corner of the grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1][2, 3].", null));
        rst.put("removeChartRowNum", new PyArg(11, "remove_chart_row", taInt, "row index in this Figure's grid. The row index starts at 0.", null));
        rst.put("removeChartColNum", new PyArg(12, "remove_chart_col", taInt, "column index in this Figure's grid. The row index starts at 0.", null));

        ////////////////////////////////////////////////////////////////


        //        rst.put("categories", new PyArg(3, "categories", taDataCategory, "discrete data or column name", null));
//        rst.put("values", new PyArg(4, "values", taDataNumeric, "numeric data or column name", null));

//
//
//        rst.put("id", new PyArg(10, "axes", taInt, "identifier", null));
//        rst.put("name", new PyArg(10, "name", taStr, "name", null));
//        rst.put("names", new PyArg(10, "names", taStrs, "series names", null));
//        rst.put("dim", new PyArg(10, "dim", taInt, "dimension of the axis", null));
//        rst.put("color", new PyArg(10, "color", taColor, "color", null));
//        rst.put("colors", new PyArg(10, "colors", taColors, "colors", null));
//        rst.put("format", new PyArg(10, "format", taAxisFormat, "axis format", null));
//        rst.put("pattern", new PyArg(10, "pattern", taStr, "axis format pattern", null));
//        rst.put("label", new PyArg(10, "label", taStr, "label", null));
//        rst.put("labels", new PyArg(10, "labels", taStrs, "labels", null));
//        rst.put("family", new PyArg(10, "family", taStr, "font family; if null, set to Arial", null));
//        rst.put("font", new PyArg(10, "font", taFont, "font", null));
//        rst.put("size", new PyArg(10, "size", taInt, "size", null));
//        rst.put("style", new PyArg(10, "style", taStr, "style", null));
//        rst.put("calendar", new PyArg(10, "calendar", taBusinessCalendar, "business calendar", null));
//        rst.put("valueColumn", new PyArg(10, "values", taStr, "column name", null));
//        rst.put("rowNum", new PyArg(10, "row", taInt, "row index in the Figure's grid. The row index starts at 0.", null));
//        rst.put("colNum", new PyArg(10, "col", taInt, "column index in this Figure's grid. The column index starts at 0.", null));
//        rst.put("index", new PyArg(10, "index", taInt, "index from the Figure's grid. The index starts at 0 in the upper left hand corner of the grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1] [2, 3].", null));
//        rst.put("showColumnNamesInTitle", new PyArg(10, "show_column_names_in_title", taBool, "whether to show column names in title. If this is true, the title format will include the column name before the comma separated values; otherwise only the comma separated values will be included.", null));
//        rst.put("title", new PyArg(10, "title", taStr, "title", null));
//        rst.put("titleColumns", new PyArg(11, "title_columns", taStrs, "columns to include in the chart title", null));
//        rst.put("titleFormat", new PyArg(12, "title_format", taStr, "a java.text.MessageFormat format string for the chart title", null));
//        rst.put("n", new PyArg(10, "width", taInt, "how many columns wide", null));
//        rst.put("npoints", new PyArg(10, "npoints", taInt, "number of points", null));
//        rst.put("visible", new PyArg(10, "visible", taInt, "true to draw the design element; false otherwise.", null));
//        rst.put("invert", new PyArg(10, "invert", taBool, "if true, larger values will be closer to the origin; otherwise, smaller values will be closer to the origin.", null));
//        rst.put("useLog", new PyArg(10, "use_log", taBool, "true to use a log axis transform; false to use a linear axis transform.", null));
//        rst.put("useBusinessTime", new PyArg(11, "use_business_time", taBool, "true to use a business time transform with the default calendar; false to use a linear axis transform.", null));
//        rst.put("maxTitleRows", new PyArg(10, "max_rows", taInt, "maximum number of row values to show in title", null));
//        rst.put("min", new PyArg(10, "min", taFloat, "range minimum", null));
//        rst.put("max", new PyArg(11, "max", taFloat, "range maximum", null));
//        rst.put("count", new PyArg(10, "count", taInt, "number of minor ticks between consecutive major ticks.", null));
//
//        //todo is time the right x-axis label?  Maybe generalize to x?
//        rst.put("time", new PyArg(2, "time", taDataTime, "time x-values.", null));
//        rst.put("open", new PyArg(3, "open", taDataNumeric, "bar open y-values.", null));
//        rst.put("high", new PyArg(4, "high", taDataNumeric, "bar high y-values.", null));
//        rst.put("low", new PyArg(5, "low", taDataNumeric, "bar low y-values.", null));
//        rst.put("close", new PyArg(6, "close", taDataNumeric, "bar close y-values.", null));
//
//        rst.put("orientation", new PyArg(10, "orientation", taStr, "plot orientation.", null));
//
//
//        rst.put("rowSpan", new PyArg(10, "row_span", taInt, "how many rows high.", null));
//        rst.put("colSpan", new PyArg(11, "col_span", taInt, "how many rows wide.", null));
//        rst.put("angle", new PyArg(10, "angle", taInt, "angle in degrees.", null));
//
//        rst.put("gapBetweenTicks", new PyArg(10, "gap", taFloat, "distance between ticks.", null));
//        rst.put("tickLocations", new PyArg(10, "loc", taFloats, "coordinates of the major tick locations.", null));
//        rst.put("transform", new PyArg(10, "transform", taAxisTransform, "transform.", null));
//        rst.put("updateIntervalMillis", new PyArg(10, "update_millis", taInt, "update interval in milliseconds.", null));
//
//        rst.put("keys", new PyArg(20, "keys", taKey, "multi-series keys or a column name containing keys.", null));
//        rst.put("key", new PyArg(20, "key", taKey, "multi-series keys or a column name containing keys.", null));
//        rst.put("keyColumn", new PyArg(20, "key_col", taStr, "colum name specifying category values.", null));
//
//        rst.put("category", new PyArg(1, "category", taStr, "category.", null));
//        rst.put("shape", new PyArg(2, "shape", taShape, "shape.", null));
//        rst.put("shapes", new PyArg(2, "shapes", taShapes, "shapes.", null));
//
//        rst.put("factor", new PyArg(3, "size", taFactor, "size.", null));
//        rst.put("factors", new PyArg(3, "sizes", taFactors, "sizes.", null));
//        rst.put("group", new PyArg(110, "group", taInt, "group for the data series.", null));


        //

        rst.put("sds", rst.get("t"));

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

        //todo pydocs

        rst.add(new PyFunc("show", SINGLETON, new String[]{"show"}, null, "TODO pydoc"));
        rst.add(new PyFunc("save", SINGLETON, new String[]{"save"}, new String[]{"path"}, "TODO pydoc"));

//        rst.add(new PyFunc("figure", SEQUENTIAL, new String[]{"figureRemoveSeries", "removeChart", "updateInterval"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("figure_title", SEQUENTIAL, new String[]{"figureTitle", "figureTitleColor", "figureTitleFont"}, null, "TODO pydoc"));
//
//        rst.add(new PyFunc("new_chart", SINGLETON, new String[]{"newChart"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("chart", SEQUENTIAL, new String[]{"chart", "chartRemoveSeries", "span", "rowSpan", "colSpan", "plotOrientation", "gridLinesVisible", "xGridLinesVisible", "yGridLinesVisible", "piePercentLabelFormat"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("chart_title", SEQUENTIAL, new String[]{"chartTitle", "chartTitleColor", "chartTitleFont", "maxRowsInTitle"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("chart_legend", SEQUENTIAL, new String[]{"legendColor", "legendFont", "legendVisible"}, null, "TODO pydoc"));
//
//        rst.add(new PyFunc("new_axes", SINGLETON, new String[]{"newAxes"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("axes", SEQUENTIAL, new String[]{"axes", "axesRemoveSeries", "plotStyle"}, null, "TODO pydoc"));
//
//        rst.add(new PyFunc("axis", SEQUENTIAL, new String[]{"axis", "axisColor", "axisFormat", "axisFormatPattern", "axisLabel", "axisLabelFont", "invert", "log", "min", "max", "range", "businessTime", "transform"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("ticks", SEQUENTIAL, new String[]{"ticks", "ticksFont", "ticksVisible", "tickLabelAngle"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("ticks_minor", SEQUENTIAL, new String[]{"minorTicks", "minorTicksVisible"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("twin", SINGLETON, new String[]{"twin"}, null, "TODO pydoc"));
//
//        rst.add(new PyFunc("x_axis", SEQUENTIAL, new String[]{"xAxis", "xColor", "xFormat", "xFormatPattern", "xLabel", "xLabelFont", "xInvert", "xLog", "xMin", "xMax", "xRange", "xBusinessTime", "xTransform"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("x_ticks", SEQUENTIAL, new String[]{"xTicks", "xTicksFont", "xTicksVisible", "xTickLabelAngle"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("x_ticks_minor", SEQUENTIAL, new String[]{"xMinorTicks", "xMinorTicksVisible"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("x_twin", SINGLETON, new String[]{"twinX"}, null, "TODO pydoc"));
//
//        rst.add(new PyFunc("y_axis", SEQUENTIAL, new String[]{"yAxis", "yColor", "yFormat", "yFormatPattern", "yLabel", "yLabelFont", "yInvert", "yLog", "yMin", "yMax", "yRange", "yBusinessTime", "yTransform"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("y_ticks", SEQUENTIAL, new String[]{"yTicks", "yTicksFont", "yTicksVisible", "yTickLabelAngle"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("y_ticks_minor", SEQUENTIAL, new String[]{"yMinorTicks", "yMinorTicksVisible"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("y_twin", SINGLETON, new String[]{"twinY"}, null, "TODO pydoc"));
//
//        rst.add(new PyFunc("series", SEQUENTIAL, new String[]{"series", "group", "seriesColor", "toolTipPattern", "xToolTipPattern", "yToolTipPattern", "errorBarColor", "gradientVisible", "seriesNamingFunction"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("point", SEQUENTIAL, new String[]{"pointColor", "pointLabel", "pointLabelFormat", "pointShape", "pointSize", "pointsVisible"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("line", SEQUENTIAL, new String[]{"lineColor", "lineStyle", "linesVisible"}, null, "TODO pydoc"));
//        rst.add(new PyFunc("func", SEQUENTIAL, new String[]{"funcNPoints", "funcRange"}, null, "TODO pydoc"));
//
        rst.add(new PyFunc("plot_xy", SINGLETON, new String[]{"plot", "plotBy", "errorBarX", "errorBarXBy", "errorBarY", "errorBarYBy", "errorBarXY", "errorBarXYBy"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("plot_xy_hist", SINGLETON, new String[]{"histPlot"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("plot_cat", SINGLETON, new String[]{"catPlot", "catPlotBy", "catErrorBar", "catErrorBarBy"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("plot_cat_hist", SINGLETON, new String[]{"catHistPlot"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("plot_pie", SINGLETON, new String[]{"piePlot"}, new String[]{"series_name"}, "TODO pydoc"));
//        rst.add(new PyFunc("plot_ohlc", SINGLETON, new String[]{"ohlcPlot", "ohlcPlotBy"}, new String[]{"series_name"}, "TODO pydoc"));

        ////////////////////////////////////////////////////////////////

        // Don't think that pointColorInteger gets used, so python code is not generated
        rst.add(new PyFunc("point_color_integer", SINGLETON, new String[]{"pointColorInteger"}, new String[]{"colors"}, "TODO pydoc", false));

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

    public static void main(String[] args) throws ClassNotFoundException, IOException {

        // generatePy removes values from signatures that it uses to generate code
        // as a result the signatures left over after calling generatePy are cases that
        // do not have generators or excludes and need to be handled
        final Map<Key, ArrayList<JavaFunction>> signatures = getMethodSignatures();
        final int nSig1 = signatures.size();
        String pyCode = generatePy(signatures);
        final int nSig2 = signatures.size();
        final int nSigGenerated = nSig1 - nSig2;

        System.out.println("GENSTATS: " + nSigGenerated + " of " + nSig1 + "(" + (nSigGenerated / (double) nSig1) + ")");

        for (int i = 0; i < 10; i++) {
            System.out.println("===========================================================");
        }

        //todo output pyCode
        System.out.println(pyCode);

        for (int i = 0; i < 10; i++) {
            System.out.println("===========================================================");
        }

        if (!signatures.isEmpty()) {
            printMethodSignatures(signatures);
            throw new RuntimeException("Not all methods were generated.");
        }
    }
}
