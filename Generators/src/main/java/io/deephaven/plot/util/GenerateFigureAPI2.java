package io.deephaven.plot.util;

import io.deephaven.libs.GroovyStaticImportGenerator.JavaFunction;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

public class GenerateFigureAPI2 {

    private static final String INDENT = "    ";

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
     * A Python input parameter.
     */
    private static class PyParameter {
        private final String name;
        private final String typeAnnotation;
        private final String defaultValue;
        private final String docString;
        //todo conversion function?

        public PyParameter(final String name, final String typeAnnotation, final String defaultValue, final String docString) {
            this.name = name;
            this.typeAnnotation = typeAnnotation;
            this.defaultValue = defaultValue;
            this.docString = docString;
        }

        @Override
        public String toString() {
            return "PyParameter{" +
                    "name='" + name + '\'' +
                    ", typeAnnotation='" + typeAnnotation + '\'' +
                    ", defaultValue='" + defaultValue + '\'' +
                    ", docString='" + docString + '\'' +
                    '}';
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

        rst.put("seriesName", new PyParameter("series_name", "str", null, "name of the created dataset"));
        rst.put("function", new PyParameter("function", "Callable", "None", "function to plot"));
        rst.put("t", new PyParameter("t", "Union[Table,SelectableDataSet]", "None", "table or selectable data set (e.g. OneClick filterable table)"));
        rst.put("x", new PyParameter("x", "Union[str,List[int],List[float],List[DateTime]]", "None", "x-values or column name"));
        rst.put("y", new PyParameter("y", "Union[str,List[int],List[float],List[DateTime]]", "None", "y-values or column name"));
        rst.put("hasXTimeAxis", new PyParameter("has_x_time_axis", "bool", "None", "whether to treat the x-values as time data")); //todo needed
        rst.put("hasYTimeAxis", new PyParameter("has_y_time_axis", "bool", "None", "whether to treat the y-values as time data")); //todo needed?

        //

        rst.put("sds", rst.get("t"));

        return rst;
    }

    /**
     * Gets the signatures of public io.deephaven.plot.Figure methods.
     *
     * @return Map of method keys to a list of all relevant signatures.
     * @throws ClassNotFoundException
     */
    public static Map<Key, ArrayList<JavaFunction>> getSignatures() throws ClassNotFoundException {
        final String imp = "io.deephaven.plot.Figure";
        final Class<?> c = Class.forName(imp);
        final Map<Key, ArrayList<JavaFunction>> signatures = new TreeMap<>();

        for (final Method m : c.getMethods()) {
            if (!m.getReturnType().getTypeName().equals(imp)) {
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

    private static Set<PyParameter> pyMethodArgs(final ArrayList<JavaFunction> signatures) {
        final Map<String, PyParameter> pyparams = getPyParameters();

        //todo figure out arg order -- maybe have ranking in pyargs

        final Set<PyParameter> args = new HashSet<>();

        for (final JavaFunction f : signatures) {
            for (final String param : f.getParameterNames()) {
                final PyParameter pyparam = pyparams.get(param);
                args.add(pyparam);
            }
        }

        return args;
    }

    private static String pyFuncSignature(final Key key, final Set<PyParameter> args) {
        final StringBuilder sb = new StringBuilder();

        sb.append("def ").append(camelToSnake(key.name)).append("(\n");

        int count = 0;
        for (final PyParameter arg : args) {
            sb.append(INDENT).append(arg.name).append(": ").append(arg.typeAnnotation);

            if (arg.defaultValue != null) {
                sb.append(" = ").append(arg.defaultValue);
            }

            sb.append(",\n");

            count++;
        }

        sb.append(") -> Figure:\n");

        return sb.toString();
    }

    private static String pyDocString(final Set<PyParameter> args){
        final StringBuilder sb = new StringBuilder();

        //todo doc string
        sb.append(INDENT).append("\"\"\"").append("TODO: doc string here").append("\n");
        sb.append(INDENT).append("Args:\n");

        for (final PyParameter arg : args) {
            sb.append(INDENT).append(INDENT).append(arg.name).append(" (").append(arg.typeAnnotation).append("): ").append(arg.docString).append("\n");
        }

        sb.append("\n").append(INDENT).append("Returns:\n").append(INDENT).append(INDENT).append("a new Figure\n");

        sb.append(INDENT).append("\"\"\"\n");

        return sb.toString();
    }

    private static String generatePythonFunction(final Key key, final ArrayList<JavaFunction> signatures) {
        final StringBuilder sb = new StringBuilder();

        // Get args

        final Set<PyParameter> args = pyMethodArgs(signatures);

        // Generate function signature

        final String sig = pyFuncSignature(key, args);
        sb.append(sig);

        // Generate docs

        final String pydocs = pyDocString(args);
        sb.append(pydocs);

        // Generate function body

        sb.append(INDENT).append("pass;\n\n");

        // return result

        return sb.toString();
    }

    public static void main(String[] args) throws ClassNotFoundException {

        final Map<Key, ArrayList<JavaFunction>> signatures = getSignatures();

        for (Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
            printSignature(entry.getKey(), entry.getValue());
        }

        for (int i = 0; i < 10; i++) {
            System.out.println("===========================================================");
        }

        String pyCode = "";

        for (Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
            final Key key = entry.getKey();
            final ArrayList<JavaFunction> sigs = entry.getValue();

            if (!key.name.equals("plot")) {
                continue;
            }

            printSignature(key, sigs);

            final String pyFunc = generatePythonFunction(key, sigs);
            pyCode += "\n" + pyFunc;
        }

        System.out.println(pyCode);

    }
}
