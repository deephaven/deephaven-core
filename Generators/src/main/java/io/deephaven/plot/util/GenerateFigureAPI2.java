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
     * A Python input parameter.
     */
    private static class PyParameter {
        private final int precedence;
        private final String name;
        private final String[] typeAnnotations;
        private final boolean required;
        private final String docString;
        private final String javaConverter;

        public PyParameter(final int precedence, final String name, final String[] typeAnnotations, final boolean required, final String docString, final String javaConverter) {
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
            return typeAnnotations.length == 1 ? typeAnnotations[0] : "Union[" + String.join(",", typeAnnotations) + "]";
        }

        /**
         * Returns a list of types string.
         *
         * @return list of types string
         */
        public String typeList() {
            return "[" + String.join(",", typeAnnotations) + "]";
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

    private static Set<String> getImplemented() {
        //todo remove plot filter
        final Set<String> set = new HashSet<>();
        set.add("plot");
        set.add("axes");
        set.add("axesRemoveSeries");
        set.add("axis");
        set.add("axisColor");
        set.add("axisFormat");
        set.add("axisFormatPattern");
        set.add("axisLabel");
        set.add("axisLabelFont");
        set.add("businessTime");
        set.add("catErrorBar");
        set.add("catErrorBarBy"); //todo combine with catErrorBar?
        set.add("catHistPlot");
        set.add("catPlot");
        set.add("catPlotBy"); //todo combine with catPlot?
        set.add("chart");
        set.add("chartRemoveSeries");
        set.add("chartTitle");
        set.add("chartTitleColor");
        set.add("chartTitleFont");
        set.add("colSpan");

        return set;
    }

    private static Map<String, PyParameter> getPyParameters() {
        final Map<String, PyParameter> rst = new HashMap<>();

        final String[] taStr = new String[]{"str"};
        final String[] taStrs = new String[]{"List[str]"};
        final String[] taBool = new String[]{"bool"};
        final String[] taInt = new String[]{"int"};
        final String[] taFloat = new String[]{"float"};
        final String[] taCallable = new String[]{"Callable"};
        final String[] taTable = new String[]{"Table","SelectableDataSet"};
        final String[] taDataCategory = new String[]{"str", "List[str]"};
        final String[] taDataNumeric = new String[]{"str", "List[int]", "List[float]", "List[DateTime]"};

        final String[] taColor = new String[]{"str", "Color"}; //todo support Color (io.deephaven.gui.color.Paint)
        final String[] taAxisFormat = new String[]{"AxisFormat"}; //todo support io.deephaven.plot.axisformatters.AxisFormat
        final String[] taFont = new String[]{"Font"}; //todo support io.deephaven.plot.Font
        final String[] taBusinessCalendar = new String[]{"BusinessCalendar"}; //todo support io.deephaven.time.calendar.BusinessCalendar

        rst.put("seriesName", new PyParameter(1, "series_name", taStr, true, "name of the created dataset", null));
        rst.put("byColumns", new PyParameter(1, "by", taStrs, false, "columns that hold grouping data", null));
        rst.put("t", new PyParameter(2, "t", taTable, false, "table or selectable data set (e.g. OneClick filterable table)", null));
        rst.put("x", new PyParameter(3, "x", taDataNumeric, false, "x-values or column name", null));
        rst.put("y", new PyParameter(4, "y", taDataNumeric, false, "y-values or column name", null));
        rst.put("function", new PyParameter(5, "function", taCallable, false, "function to plot", null));
        rst.put("hasXTimeAxis", new PyParameter(6, "has_x_time_axis", taBool, false, "whether to treat the x-values as time data", null)); //todo needed
        rst.put("hasYTimeAxis", new PyParameter(7, "has_y_time_axis", taBool, false, "whether to treat the y-values as time data", null)); //todo needed?

        rst.put("categories", new PyParameter(3, "categories", taDataCategory, false, "discrete data or column name", null));
        rst.put("values", new PyParameter(4, "values", taDataNumeric, false, "numeric data or column name", null));
        rst.put("yLow", new PyParameter(5, "y_low", taDataNumeric, false, "low value in y dimension", null));
        rst.put("yHigh", new PyParameter(6, "y_high", taDataNumeric, false, "high value in y dimension", null));

        rst.put("id", new PyParameter(10, "axes", taInt, false, "axes id", null));
        rst.put("name", new PyParameter(10, "name", taStr, false, "axes name", null));
        rst.put("names", new PyParameter(10, "names", taStrs, true, "series names", null));
        rst.put("dim", new PyParameter(10, "dim", taInt, true, "dimension of the axis", null));
        rst.put("color", new PyParameter(10, "color", taColor, true, "color", null));
        rst.put("format", new PyParameter(10, "format", taAxisFormat, true, "axis format", null));
        rst.put("pattern", new PyParameter(10, "pattern", taStr, true, "axis format pattern", null));
        rst.put("label", new PyParameter(10, "label", taStr, true, "label", null));
        rst.put("family", new PyParameter(10, "family", taStr, false, "font family; if null, set to Arial", null));
        rst.put("font", new PyParameter(10, "font", taFont, false, "font", null));
        rst.put("size", new PyParameter(10, "size", taInt, false, "font size", null));
        rst.put("style", new PyParameter(10, "style", taStr, false, "font style", null));
        rst.put("calendar", new PyParameter(10, "calendar", taBusinessCalendar, false, "business calendar", null));
        rst.put("valueColumn", new PyParameter(10, "values", taStr, false, "column name", null));
        rst.put("rowNum", new PyParameter(10, "row", taInt, false, "row index in the Figure's grid. The row index starts at 0.", null));
        rst.put("colNum", new PyParameter(10, "col", taInt, false, "column index in this Figure's grid. The column index starts at 0.", null));
        rst.put("index", new PyParameter(10, "index", taInt, false, "index from the Figure's grid. The index starts at 0 in the upper left hand corner of the grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1] [2, 3].", null));
        rst.put("showColumnNamesInTitle", new PyParameter(10, "show_column_names_in_title", taBool, false, "whether to show column names in title. If this is true, the title format will include the column name before the comma separated values; otherwise only the comma separated values will be included.", null));
        rst.put("title", new PyParameter(10, "title", taStr, false, "title", null));
        rst.put("titleColumns", new PyParameter(11, "title_columns", taStrs, false, "columns to include in the chart title", null));
        rst.put("titleFormat", new PyParameter(12, "title_format", taStr, false, "a java.text.MessageFormat format string for the chart title", null));
        rst.put("n", new PyParameter(10, "width", taInt, false, "how many columns wide", null));

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
        final Map<String,PyParameter> pyParameterMap = getPyParameters();

        final StringBuilder sb = new StringBuilder();

        // validate

        for(final PyParameter arg:args){
            if(!arg.required){
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
                    .append(arg.typeList())
                    .append(")\n");
        }

        sb.append("\n");

        // function calls

        final List<String[]> argNames = javaArgNames(signatures);

        boolean isFirst = true;

        for(final String[] an : argNames){
            final String[] quoted_an = Arrays.stream(an).map(s-> "\""+pyParameterMap.get(s).name+"\"").toArray(String[]::new);

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
                    .append(String.join(",", Arrays.stream(an).map(s->pyParameterMap.get(s).name).toArray(String[]::new)))
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
        final Set<String> filter = getImplemented();

        for (Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
            final Key key = entry.getKey();
            final ArrayList<JavaFunction> sigs = entry.getValue();

            //todo remove plot filter
            if (!filter.contains(key.name)) {
                continue;
            }

            //todo remove printSignature
//            printSignature(key, sigs);

            final String pyFunc = generatePythonFunction(key, sigs);

            //todo remove print pyFunc
//            System.out.println("\n");
//            System.out.println(pyFunc);

            sb.append("\n").append(pyFunc);
            nGenerated++;
        }

        //todo remove print
        System.out.println("GENSTATS: " + nGenerated + " of " + signatures.size() + "(" + (nGenerated/(double)signatures.size()) + ")");

        return sb.toString();
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {

        final Map<Key, ArrayList<JavaFunction>> signatures = getSignatures();

        //todo remove filter
        final Set<String> implemented = getImplemented();

        for (Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
            if(!implemented.contains(entry.getKey().name)) {
                printSignature(entry.getKey(), entry.getValue());
            }
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
