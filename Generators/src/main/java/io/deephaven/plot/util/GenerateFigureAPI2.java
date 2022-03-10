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

public class GenerateFigureAPI2 {

    private static final String INDENT = "    ";
    private static final String JCLASS = "io.deephaven.plot.Figure";
    private static final String PREAMBLE = "Generators/src/main/java/io/deephaven/pythonPreambles/plotV2.py";

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
     * A Python type annotation.
     */
    private static class PyTypeAnnotation {
        private final String name;
        private final String[] specializations;

        public PyTypeAnnotation(String name, String... specializations) {
            this.name = name;
            this.specializations = specializations;
        }

        @Override
        public String toString() {
            return "PyTypeAnnotation{" +
                    "name='" + name + '\'' +
                    ", specializations=" + Arrays.toString(specializations) +
                    '}';
        }

        public List<String> annotations() {
            final ArrayList<String> rst = new ArrayList<>();

            if(specializations.length == 0){
                rst.add(name);
            } else {
                for (String s : specializations) {
                    rst.add(name + "[" + s + "]");
                }
            }

            return rst;
        }
    }

    /**
     * A Python input parameter.
     */
    private static class PyParameter {
        private final int precedence;
        private final String name;
        private final PyTypeAnnotation[] typeAnnotations;
        private final boolean required;
        private final String docString;
        private final String javaConverter;

        public PyParameter(final int precedence, final String name, final PyTypeAnnotation[] typeAnnotations, final boolean required, final String docString, final String javaConverter) {
            this.precedence = precedence;
            this.name = name;
            this.typeAnnotations = typeAnnotations;
            this.required = required;
            this.docString = docString;
            this.javaConverter = javaConverter;
        }

        @Override
        public String toString() {
            return "PyParameter{" +
                    "precedence=" + precedence +
                    ", name='" + name + '\'' +
                    ", typeAnnotations='" + Arrays.toString(typeAnnotations) + '\'' +
                    ", required=" + required +
                    ", docString='" + docString + '\'' +
                    ", javaConverter='" + javaConverter + '\'' +
                    '}';
        }

        /**
         * Returns the type annotation string.
         *
         * @return type annotation string
         */
        public String typeAnnotation() {
            final List<String> annotations = Arrays.stream(typeAnnotations)
                    .flatMap(ta -> ta.annotations().stream())
                    .collect(Collectors.toList());

            return annotations.size() == 1 ? annotations.get(0) : "Union[" + String.join(",", annotations) + "]";
        }

        /**
         * Returns the type assertion tuple string.
         *
         * @return type assertion tuple string
         */
        public String typeAssertion() {
            final List<String> types = Arrays.stream(typeAnnotations)
                    .map(ta -> ta.name)
                    .collect(Collectors.toList());

            return "(" + String.join(",", types) + ")";
        }

    }

    /**
     * Convert camel case to snake case.
     *
     * @param str input
     * @return snake case string
     */
    private static String camelToSnake(final String str) {
        String regex = "([a-z])([A-Z]+)";
        String replacement = "$1_$2";
        return str.replaceAll(regex, replacement).toLowerCase();
    }

    private static Map<String, PyParameter> getPyParameters() {
        final Map<String, PyParameter> rst = new HashMap<>();

        rst.put("seriesName", new PyParameter(1, "series_name", new PyTypeAnnotation[]{new PyTypeAnnotation("str")}, true, "name of the created dataset", null));
        rst.put("t", new PyParameter(2, "t", new PyTypeAnnotation[]{new PyTypeAnnotation("Table"), new PyTypeAnnotation("SelectableDataSet")}, false, "table or selectable data set (e.g. OneClick filterable table)", null));
        rst.put("x", new PyParameter(3, "x", new PyTypeAnnotation[]{new PyTypeAnnotation("str"), new PyTypeAnnotation("List", "int", "float", "DateTime")}, false, "x-values or column name", null));
        rst.put("y", new PyParameter(4, "y", new PyTypeAnnotation[]{new PyTypeAnnotation("str"), new PyTypeAnnotation("List", "int", "float", "DateTime")}, false, "y-values or column name", null));
        rst.put("function", new PyParameter(5, "function", new PyTypeAnnotation[]{new PyTypeAnnotation("Callable")}, false, "function to plot", null));
        rst.put("hasXTimeAxis", new PyParameter(6, "has_x_time_axis", new PyTypeAnnotation[]{new PyTypeAnnotation("bool")}, false, "whether to treat the x-values as time data", null)); //todo needed
        rst.put("hasYTimeAxis", new PyParameter(7, "has_y_time_axis", new PyTypeAnnotation[]{new PyTypeAnnotation("bool")}, false, "whether to treat the y-values as time data", null)); //todo needed?

        //

        rst.put("sds", rst.get("t"));

        return rst;
    }

    /**
     * Gets the signatures of public JCLASS methods.
     *
     * @return Map of method keys to a list of all relevant signatures.
     * @throws ClassNotFoundException JCLASS is not found
     */
    public static Map<Key, ArrayList<JavaFunction>> getSignatures() throws ClassNotFoundException {
        final Class<?> c = Class.forName(JCLASS);
        final Map<Key, ArrayList<JavaFunction>> signatures = new TreeMap<>();

        for (final Method m : c.getMethods()) {
            if (!m.getReturnType().getTypeName().equals(JCLASS)) {
                // only look at methods of the plot builder
                continue;
            }

            final Key key = new Key(m);
            final JavaFunction f = new JavaFunction(m);

            if (key.isPublic) {
                final ArrayList<JavaFunction> sigs = signatures.computeIfAbsent(key, k -> new ArrayList<>());
                sigs.add(f);
            }
        }

        return signatures;
    }

    /**
     * Prints details for a method signature.
     *
     * @param key        signature key
     * @param signatures list of signatures
     */
    private static void printSignature(final Key key, final ArrayList<JavaFunction> signatures) {
        System.out.println("-----------------------------------------------------------------------");

        System.out.println("Name: " + key.name);
        System.out.println("IsPublic: " + key.isPublic);
        System.out.println("IsStatic: " + key.isStatic);

        final Set<String> returnTypes = new TreeSet<>();
        final Map<String, Set<String>> params = new TreeMap<>();

        for (final JavaFunction f : signatures) {
            returnTypes.add(f.getReturnType().getTypeName());

            for (int i = 0; i < f.getParameterNames().length; i++) {
                final Set<String> paramTypes = params.computeIfAbsent(f.getParameterNames()[i], n -> new TreeSet<>());
                paramTypes.add(f.getParameterTypes()[i].getTypeName());
            }
        }

        System.out.println("ReturnTypes: ");

        for (String returnType : returnTypes) {
            System.out.println("\t" + returnType);
        }

        System.out.println("Params:");

        for (Map.Entry<String, Set<String>> entry : params.entrySet()) {
            System.out.println("\t" + entry.getKey() + "=" + entry.getValue());
        }

        System.out.println("Signatures:");

        for (final JavaFunction f : signatures) {
            StringBuilder sig = new StringBuilder(f.getReturnType().getTypeName());
            sig.append(" (");

            for (int i = 0; i < f.getParameterNames().length; i++) {
                if (i > 0) {
                    sig.append(", ");
                }

                sig.append(f.getParameterNames()[i]).append("=").append(f.getParameterTypes()[i].getTypeName());
            }

            sig.append(")");

            System.out.println("\t" + sig);
        }
    }

    private static List<PyParameter> pyMethodArgs(final ArrayList<JavaFunction> signatures) {
        final Map<String, PyParameter> pyparams = getPyParameters();
        final Set<PyParameter> argSet = new HashSet<>();

        for (final JavaFunction f : signatures) {
            for (final String param : f.getParameterNames()) {
                final PyParameter pyparam = pyparams.get(param);

                if(pyparam == null){
                    throw new IllegalArgumentException("Unsupported python parameter: " + param);
                }

                argSet.add(pyparam);
            }
        }

        final List<PyParameter> args = new ArrayList<>(argSet);
        args.sort((a,b)->Integer.compare(a.precedence,b.precedence));

        return args;
    }

    private static String pyFuncSignature(final Key key, final List<PyParameter> args) {
        final StringBuilder sb = new StringBuilder();

        sb.append(INDENT).append("def ").append(camelToSnake(key.name)).append("(\n");
        sb.append(INDENT).append(INDENT).append("self,\n");

        for (final PyParameter arg : args) {
            sb.append(INDENT).append(INDENT).append(arg.name).append(": ").append(arg.typeAnnotation());

            if (!arg.required) {
                sb.append(" = None");
            }

            sb.append(",\n");
        }

        sb.append(INDENT).append(") -> Figure:\n");

        return sb.toString();
    }

    private static String pyDocString(final List<PyParameter> args){
        final StringBuilder sb = new StringBuilder();

        //todo doc string
        sb.append(INDENT).append(INDENT).append("\"\"\"").append("TODO: doc string here").append("\n");
        sb.append(INDENT).append(INDENT).append("Args:\n");

        for (final PyParameter arg : args) {
            sb.append(INDENT).append(INDENT).append(INDENT).append(arg.name).append(" (").append(arg.typeAnnotation()).append("): ").append(arg.docString).append("\n");
        }

        sb.append("\n").append(INDENT).append(INDENT).append("Returns:\n").append(INDENT).append(INDENT).append(INDENT).append("a new Figure\n");

        sb.append("\n").append(INDENT).append(INDENT).append("Raises:\n").append(INDENT).append(INDENT).append(INDENT).append("DHError\n");

        sb.append(INDENT).append(INDENT).append("\"\"\"\n");

        return sb.toString();
    }

    /**
     * Gets the valid Java method argument name combinations.
     *
     * @param signatures java functions with the same name.
     * @return valid Java method argument name combinations.
     */
    private static List<String[]> javaArgNames(final ArrayList<JavaFunction> signatures) {
        final Map<Set<String>, String[]> vals = new LinkedHashMap<>();

        for(JavaFunction f : signatures) {
            final String[] params = f.getParameterNames();
            final Set<String> s = new HashSet<>(Arrays.asList(params));

            if (vals.containsKey(s) && !Arrays.equals(params, vals.get(s))) {
                throw new RuntimeException("Parameters are already present: " + Arrays.toString(params));
            }

            vals.put(s, params);
        }

        return new ArrayList<>(vals.values());
    }

    private static String pyFuncBody(final Key key, final List<PyParameter> args, final ArrayList<JavaFunction> signatures) {
        final StringBuilder sb = new StringBuilder();

        // validate

        for(final PyParameter arg:args){
            if(!arg.required){
                continue;
            }

            sb.append(INDENT).append(INDENT).append("if not ").append(arg.name).append(": raise DHError(\"required parameter is not set: ").append(arg.name).append("\")\n");
        }

        sb.append("\n");

        // non-null params

        sb.append(INDENT).append(INDENT).append("non_null_params = set()\n");

        for(final PyParameter arg:args){
            sb.append(INDENT)
                    .append(INDENT)
                    .append("if ")
                    .append(arg.name)
                    .append(" is not None:\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append(INDENT)
                    .append("non_null_params.add(")
                    .append(arg.name).append(")\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append(INDENT)
                    .append(arg.name)
                    .append(" = ")
                    .append(arg.javaConverter == null ? "_convert_j" : arg.javaConverter)
                    .append("(\"")
                    .append(arg.name)
                    .append("\",")
                    .append(arg.name)
                    .append(",")
                    .append(arg.typeAssertion())
                    .append(")\n");
        }

        sb.append("\n");

        // function calls

        final List<String[]> argNames = javaArgNames(signatures);

        boolean isFirst = true;

        for(final String[] an : argNames){
            final String[] quoted_an = Arrays.stream(an).map(s-> "\""+s+"\"").toArray(String[]::new);

            sb.append(INDENT)
                    .append(INDENT)
                    .append(isFirst ? "if" : "elif")
                    .append(" non_null_params == {")
                    .append(String.join(",",quoted_an))
                    .append("}:\n")
                    .append(INDENT)
                    .append(INDENT)
                    .append(INDENT)
                    .append("return Figure(self.j_figure.")
                    .append(key.name)
                    .append("(")
                    .append(String.join(",", an))
                    .append("))\n");

            isFirst = false;
        }

        sb.append(INDENT)
                .append(INDENT)
                .append("else:\n")
                .append(INDENT)
                .append(INDENT)
                .append(INDENT)
                .append("raise DHError(f\"unsupported parameter combination: {non_null_params}\")\n");

        return sb.toString();
    }

    private static String generatePythonFunction(final Key key, final ArrayList<JavaFunction> signatures) {
        final StringBuilder sb = new StringBuilder();

        final List<PyParameter> args = pyMethodArgs(signatures);

        final String sig = pyFuncSignature(key, args);
        final String pydocs = pyDocString(args);
        final String pybody = pyFuncBody(key, args, signatures);

        sb.append(sig);
        sb.append(pydocs);
        sb.append(pybody);

        return sb.toString();
    }

    private static String generatePythonClass(final Map<Key, ArrayList<JavaFunction>> signatures) throws IOException {

        final StringBuilder sb = new StringBuilder();

        final String preamble = Files.readString(Path.of(PREAMBLE));
        sb.append(preamble);
        sb.append("\n");

        int nGenerated = 0;

        //todo remove plot filter
        final Set<String> filter = new HashSet<>();
        filter.add("plot");
//        filter.add("catPlot");

        for (Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
            final Key key = entry.getKey();
            final ArrayList<JavaFunction> sigs = entry.getValue();

            //todo remove plot filter
            if (!filter.contains(key.name)) {
                continue;
            }

            //todo remove printSignature
            printSignature(key, sigs);

            final String pyFunc = generatePythonFunction(key, sigs);

            //todo remove print pyFunc
            System.out.println("\n");
            System.out.println(pyFunc);

            sb.append("\n").append(pyFunc);
            nGenerated++;
        }

        //todo remove print
        System.out.println("GENSTATS: " + nGenerated + " of " + signatures.size() + "(" + (nGenerated/(double)signatures.size()) + ")");

        return sb.toString();
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {

        final Map<Key, ArrayList<JavaFunction>> signatures = getSignatures();

        for (Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
            printSignature(entry.getKey(), entry.getValue());
        }

        for (int i = 0; i < 10; i++) {
            System.out.println("===========================================================");
        }

        String pyCode = generatePythonClass(signatures);

        for (int i = 0; i < 10; i++) {
            System.out.println("===========================================================");
        }

        System.out.println(pyCode);
    }
}
