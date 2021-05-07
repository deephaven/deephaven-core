package io.deephaven.db.v2.select;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.python.ASTHelper;
import io.deephaven.python.ASTHelperSetup;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import org.jpy.PyDictWrapper;
import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyObject;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class NumbaCompileTools {

    public static final String NUMPY_ARRAY_PREFIX = "array(";
    public static final String NUMPY_ARRAY_SUFFIX_C = ", 1d, C)";
    public static final String NUMPY_ARRAY_SUFFIX_A = ", 1d, A)";

    static class NumbaEvaluator {
        private final PyObject vectorized;
        private final Class[] usedColumnsTypes;
        private final String[] columnsNpTypeName;

        NumbaEvaluator(PyObject vectorized, Class[] usedColumnsTypes, String[] columnsNpTypeName) {
            this.vectorized = vectorized;
            this.usedColumnsTypes = usedColumnsTypes;
            this.columnsNpTypeName = columnsNpTypeName;
        }


        class Context {

            private final Object[] args;
            private final ArgumentsBuilder argumentsBuilders[];
            private final ArgumentsBuilder.Context subContexts[];


            public Context() {
                int argumentsCount = 1;
                argumentsBuilders = new ArgumentsBuilder[usedColumnsTypes.length];
                subContexts = new ArgumentsBuilder.Context[usedColumnsTypes.length];
                for (int i = 0; i < usedColumnsTypes.length; i++) {
                    argumentsBuilders[i] = ArgumentsBuilder.getBuilder(usedColumnsTypes[i], columnsNpTypeName[i]);
                    argumentsCount += argumentsBuilders[i].getArgumentsCount();
                    subContexts[i] = argumentsBuilders[i].getContext();
                }
                args = new Object[argumentsCount];
            }

        }


        public PyObject numbaEvaluate(Context context, Chunk<? extends Attributes.Values>[] inputChunks, int size) {
            Object[] args = context.args;
            args[0] = size;
            int offset = 1;
            for (int i = 0; i < context.argumentsBuilders.length; i++) {
                Object[] individualArgs = context.argumentsBuilders[i].packArguments(inputChunks[i], context.subContexts[i], size);
                System.arraycopy(individualArgs, 0, args, offset, individualArgs.length);
                offset += individualArgs.length;
            }

            return vectorized.call("__call__", new Object[]{args});
        }

        public Context getContext() {
            return new Context();
        }
    }

    static PyDictWrapper compileFormula(String formula, Map<String, ColumnDefinition> columnNameMap) {
        final Set<String> formulaNames;
        try {
            formulaNames = new HashSet<>(ASTHelper.convertToList(
                ASTHelperSetup.getInstance().extract_expression_names(formula)));
        } catch (Exception e) {
            throw new FormulaCompilationException("Invalid formula:\n" + formula, e);
        }

        // IDS-6063
        // Ensure we don't pass columns that the expression doesn't even reference

        final String[] columnName = columnNameMap.entrySet()
            .stream()
            .filter(e -> formulaNames.contains(e.getKey()))
            .map(Entry::getKey)
            .toArray(String[]::new);

        final String[] columnTypePython = columnNameMap.entrySet()
            .stream()
            .filter(e -> formulaNames.contains(e.getKey()))
            .map(e -> toPyType(e.getValue().getDataType(), e.getValue().getComponentType(), true))
            .toArray(String[]::new);

        // IDS-6073
        // todo: clean this up! :O
        exec("import numpy\nimport deephaven.lang.vectorize_simple as __vectorize_simple__\n");
        try (final PyDictWrapper effectiveGlobals = getEffectiveGlobals().asDict()) {
            effectiveGlobals.setItem("__column_names__", columnName);
            effectiveGlobals.setItem("__column_types__", columnTypePython);
            effectiveGlobals.setItem("__internal_formula__", formula);
        }
        // hokay!  ready to compile vectorized function.  This will return a map like so:
        // {fun: vec_function, columns: used_cols, references: used_refs }
        try {
            return eval("__vectorize_simple__.compile_function(__internal_formula__,zip(__column_names__,__column_types__),__vectorize_simple__.export_locals(globals(),locals()))").asDict();
            //  vec.callMethod("compile_function", formula, allCols.unwrap()).asDict();
        } catch (Exception e) {
            throw new FormulaCompilationException("Invalid formula:\n" + formula, e);
        }
    }

    private static PyObject getEffectiveGlobals() {
        PyObject currentGlobals = PyLib.getCurrentGlobals();
        if (currentGlobals == null) {
            return PyLib.getMainGlobals();
        }
        return currentGlobals;
    }

    private static PyObject eval(String formula) {
        final PyObject currentGlobals = PyLib.getCurrentGlobals();
        if (currentGlobals == null) {
            return PyObject.executeCode(formula, PyInputMode.EXPRESSION);
        }
        try (final PyObject currentLocals = PyLib.getCurrentLocals()) {
            return PyObject.executeCode(formula, PyInputMode.EXPRESSION, currentGlobals, currentLocals);
        } finally {
            currentGlobals.close();
        }

    }

    private static void exec(String formula) {
        final PyObject currentGlobals = PyLib.getCurrentGlobals();
        if (currentGlobals == null) {
            //noinspection EmptyTryBlock
            try (final PyObject obj = PyObject.executeCode(formula, PyInputMode.SCRIPT)) {

            }
            return;
        }
        //noinspection EmptyTryBlock
        try (
            final PyObject currentLocals = PyLib.getCurrentLocals();
            final PyObject obj = PyObject.executeCode(formula, PyInputMode.SCRIPT, currentGlobals, currentLocals)) {

        } finally {
            currentGlobals.close();
        }
    }

    /**
     * Returns a numba recognizable type string corresponding to a DH cell type
     *
     * @param type          The main cell type
     * @param componentType The component type in case the main type is a DbArray
     * @param noFail        Indicate whether the method is supposed to throw an exception of silently return null in case of mismatch
     * @return
     */
    public static String toPyType(Class type, Class componentType, boolean noFail) {
        String suffix = "";
        if (type.isArray()) {
            suffix = "[:";
            type = type.getComponentType();
            while (type.isArray()) {
                suffix += ",:";
                type = type.getComponentType();

            }
            suffix += "]";

        }
        String result;
        if (type == String.class) {
            result = "unicode_type";
        } else if (type == double.class || type == Double.class) {
            result = "float64";
        } else if (type == float.class || type == Float.class) {
            result = "float32";
        } else if (type == long.class || type == Long.class) {
            result = "int64";
        } else if (type == int.class || type == Integer.class) {
            result = "int32";
        } else if (type == short.class || type == Short.class) {
            result = "int16";
        } else if (type == char.class || type == Character.class) {
            result = "uint16";
        } else if (type == byte.class || type == Byte.class) {
            result = "int8";
        } else if (type == boolean.class || type == Boolean.class) {
            result = "bool_";
        } else if (type.isPrimitive()) {
            if (noFail) {
                result = "object";
            }
            throw new UnsupportedOperationException(type + " not (yet) supported");
        } else if (DbArrayBase.class.isAssignableFrom(type) && componentType != null) {
            return toPyType(componentType, null, noFail) + "[:]";
        } else {
            if (noFail) {
                result = "object";
            }
            throw new UnsupportedOperationException(type + " not supported");
        }
        return result + suffix;
    }

    static final Map<String, Class> pythonToJavaType;

    public static final String pythonVersion;

    static {
        Map<String, Class> pythonToJavaTypeTemp = new HashMap<>();
        pythonToJavaTypeTemp.put("float64", double.class);
        pythonToJavaTypeTemp.put("float_", double.class);
        pythonToJavaTypeTemp.put("float32", float.class);
        pythonToJavaTypeTemp.put("int64", long.class);
        pythonToJavaTypeTemp.put("int_", long.class);
        pythonToJavaTypeTemp.put("intc", long.class);
        pythonToJavaTypeTemp.put("intp", long.class);
        pythonToJavaTypeTemp.put("uint64", long.class);
        pythonToJavaTypeTemp.put("int32", int.class);
        pythonToJavaTypeTemp.put("uint32", long.class);
        pythonToJavaTypeTemp.put("int16", short.class);
        pythonToJavaTypeTemp.put("uint16", char.class);
        pythonToJavaTypeTemp.put("uint8", byte.class);
        pythonToJavaTypeTemp.put("int8", byte.class);
        pythonToJavaTypeTemp.put("bool", Boolean.class);
        pythonToJavaTypeTemp.put("bool_", Boolean.class);
        pythonToJavaTypeTemp.put("object", Object.class);
        pythonToJavaTypeTemp.put("unicode_type", String.class);
        pythonToJavaType = Collections.unmodifiableMap(pythonToJavaTypeTemp);

        exec("import sys");
        pythonVersion = eval("sys.version.split()[0]").toString();
        if (pythonVersion.startsWith("2.")) {
            //      throw new RuntimeException("Numba support is only enabled for python 3 - current version is " + pythonVersion);
        }
    }

    public static Class toJavaType(String pythonType) {
        if (pythonType.endsWith("[:]")) {
            return Array.newInstance(pythonToJavaType.get(pythonType.substring(0, pythonType.length() - 3)), 0).getClass();
        }
        if (pythonType.startsWith(NUMPY_ARRAY_PREFIX) && (pythonType.endsWith(NUMPY_ARRAY_SUFFIX_A) || pythonType.endsWith(NUMPY_ARRAY_SUFFIX_C))){
            return Array.newInstance(pythonToJavaType.get(pythonType.substring(NUMPY_ARRAY_PREFIX.length(), pythonType.length() - NUMPY_ARRAY_SUFFIX_A.length())), 0).getClass();
        }
        return pythonToJavaType.get(pythonType);
    }

    static LinkedHashMap<String, String> findUsedColumns(
            PyDictWrapper vectorized,
            Map<String, ColumnDefinition> tableDefinition
    ) {
        final PyObject cols = vectorized.get("columns");
        final LinkedHashMap<String, String> columns = new LinkedHashMap<>();
        for (PyObject used_col : cols.asList()) {
            String colName = used_col.getStringValue();
            final ColumnDefinition col = tableDefinition.get(colName);
            final String pyType = toPyType(col.getDataType(), col.getComponentType(), true);
            columns.put(colName, pyType);
        }
        return columns;
    }

    static Map<String, ColumnDefinition> getExpandedColumnNames(TableDefinition tableDefinition) {
        Map<String, ColumnDefinition> columnNameMap = new LinkedHashMap<>(tableDefinition.getColumnNameMap());
        columnNameMap.put("i", ColumnDefinition.ofInt("i"));
        columnNameMap.put("ii", ColumnDefinition.ofLong("ii"));
        columnNameMap.put("k", ColumnDefinition.ofLong("k"));
        return columnNameMap;
    }

    static Map<String, ColumnDefinition> getExpandedColumnNames(Map<String, ColumnDefinition> tableDefinition) {
        Map<String, ColumnDefinition> columnNameMap = new LinkedHashMap<>(tableDefinition);
        columnNameMap.put("i", ColumnDefinition.ofInt("i"));
        columnNameMap.put("ii", ColumnDefinition.ofLong("ii"));
        columnNameMap.put("k", ColumnDefinition.ofLong("k"));
        return columnNameMap;
    }


}
