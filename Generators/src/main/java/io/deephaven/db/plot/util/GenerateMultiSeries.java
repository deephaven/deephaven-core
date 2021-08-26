package io.deephaven.db.plot.util;

import io.deephaven.base.ClassUtil;
import io.deephaven.base.Pair;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.plot.util.functions.ClosureFunction;
import io.deephaven.db.tables.Table;
import io.deephaven.libs.GroovyStaticImportGenerator;
import io.deephaven.util.type.TypeUtils;
import groovy.lang.Closure;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Logger;

import static io.deephaven.db.plot.util.PlotGeneratorUtils.indent;

/**
 * Generates methods for the MultiSeries datasets.
 */
public class GenerateMultiSeries {
    private static Logger log = Logger.getLogger(GenerateMultiSeries.class.toString());

    public static void main(String[] args) throws ClassNotFoundException, IOException, NoSuchMethodException {

        String devroot = null;
        boolean assertNoChange = false;
        if (args.length == 0) {
            devroot = Configuration.getInstance().getDevRootPath();
        } else if (args.length == 1) {
            devroot = args[0];
        } else if (args.length == 2) {
            devroot = args[0];
            assertNoChange = Boolean.parseBoolean(args[1]);
        } else {
            System.out.println("Usage: [<devroot> [assertNoChange]]");
            System.exit(-1);
        }

        final Set<Method> skip = new HashSet<>();
        skip.add(Class.forName("io.deephaven.db.plot.datasets.DataSeries").getMethod("pointSize", int.class));
        skip.add(Class.forName("io.deephaven.db.plot.datasets.DataSeries").getMethod("pointSize", double.class));
        skip.add(Class.forName("io.deephaven.db.plot.datasets.DataSeries").getMethod("pointSize", long.class));

        new Generator("io.deephaven.db.plot.datasets.multiseries.MultiSeries",
                "DataSeriesInternal",
                false,
                false,
                false,
                "io.deephaven.db.plot.datasets.DataSeries",
                "io.deephaven.db.plot.datasets.category.CategoryDataSeries",
                "io.deephaven.db.plot.datasets.xy.XYDataSeries",
                "io.deephaven.db.plot.datasets.ohlc.OHLCDataSeries").generateCode(devroot, assertNoChange, skip);

        new Generator("io.deephaven.db.plot.datasets.multiseries.AbstractMultiSeries",
                "DataSeriesInternal",
                true,
                false,
                false,
                "io.deephaven.db.plot.datasets.DataSeries",
                "io.deephaven.db.plot.datasets.category.CategoryDataSeries",
                "io.deephaven.db.plot.datasets.xy.XYDataSeries",
                "io.deephaven.db.plot.datasets.ohlc.OHLCDataSeries").generateCode(devroot, assertNoChange, skip);

        new Generator(
                "io.deephaven.db.plot.datasets.multiseries.MultiCatSeries",
                "CategoryDataSeriesInternal",
                false,
                false,
                false,
                "io.deephaven.db.plot.datasets.category.CategoryDataSeries").generateCode(devroot, assertNoChange,
                        skip);

        new Generator(
                "io.deephaven.db.plot.datasets.multiseries.MultiCatErrorBarSeries",
                "CategoryErrorBarDataSeriesInternal",
                false,
                false,
                false,
                "io.deephaven.db.plot.datasets.category.CategoryDataSeries").generateCode(devroot, assertNoChange,
                        skip);

        new Generator(
                "io.deephaven.db.plot.datasets.multiseries.MultiCatErrorBarSeriesSwappable",
                "CategoryErrorBarDataSeriesInternal",
                false,
                false,
                true,
                "io.deephaven.db.plot.datasets.category.CategoryDataSeries").generateCode(devroot, assertNoChange,
                        skip);

        new Generator(
                "io.deephaven.db.plot.datasets.multiseries.MultiXYErrorBarSeries",
                "XYErrorBarDataSeriesInternal",
                false,
                false,
                false,
                "io.deephaven.db.plot.datasets.xy.XYDataSeries").generateCode(devroot, assertNoChange, skip);
        new Generator(
                "io.deephaven.db.plot.datasets.multiseries.MultiXYErrorBarSeriesSwappable",
                "XYErrorBarDataSeriesInternal",
                false,
                false,
                true,
                "io.deephaven.db.plot.datasets.xy.XYDataSeries").generateCode(devroot, assertNoChange, skip);

        new Generator(
                "io.deephaven.db.plot.datasets.multiseries.MultiCatSeriesSwappable",
                "CategoryDataSeriesInternal",
                false,
                false,
                true,
                "io.deephaven.db.plot.datasets.category.CategoryDataSeries").generateCode(devroot, assertNoChange,
                        skip);

        new Generator("io.deephaven.db.plot.datasets.multiseries.MultiXYSeries",
                "XYDataSeriesInternal",
                false,
                false,
                false,
                "io.deephaven.db.plot.datasets.xy.XYDataSeries").generateCode(devroot, assertNoChange, skip);

        new Generator("io.deephaven.db.plot.datasets.multiseries.MultiXYSeriesSwappable",
                "XYDataSeriesInternal",
                false,
                false,
                true,
                "io.deephaven.db.plot.datasets.xy.XYDataSeries").generateCode(devroot, assertNoChange, skip);

        new Generator("io.deephaven.db.plot.datasets.multiseries.MultiOHLCSeries",
                "OHLCDataSeriesInternal",
                false,
                false,
                false,
                "io.deephaven.db.plot.datasets.ohlc.OHLCDataSeries").generateCode(devroot, assertNoChange, skip);

        new Generator("io.deephaven.db.plot.datasets.multiseries.MultiOHLCSeriesSwappable",
                "OHLCDataSeriesInternal",
                false,
                false,
                true,
                "io.deephaven.db.plot.datasets.ohlc.OHLCDataSeries").generateCode(devroot, assertNoChange, skip);
    }

    static class Generator {
        private final String outputClass;
        private final String parameterizedType;
        private final boolean isGeneric;
        private final boolean isTransform;
        private final boolean isSwappable;
        private final String[] interfaces;
        private final boolean isInterface;
        private final boolean isAbstract;
        private final Class output;

        Generator(final String outputClass, final String parameterizedType, final boolean isGeneric,
                final boolean isTransform, final boolean isSwappable, final String... interfaces)
                throws ClassNotFoundException {
            this.outputClass = outputClass;
            this.parameterizedType = parameterizedType;
            this.isGeneric = isGeneric;
            this.isTransform = isTransform;
            this.isSwappable = isSwappable;
            this.interfaces = interfaces;
            output = Class.forName(outputClass);

            final int mod = output.getModifiers();
            isInterface = Modifier.isInterface(mod);
            isAbstract = Modifier.isAbstract(mod);
        }


        private void generateCode(final String devroot, final boolean assertNoChange, final Set<Method> skip)
                throws ClassNotFoundException, IOException {
            final StringBuilder code = new StringBuilder();
            final String outputFile = devroot + "/Plot/src/main/java/" + outputClass.replace(".", "/") + ".java";


            final String headerMessage = "CODE BELOW HERE IS GENERATED -- DO NOT EDIT BY HAND";
            final String headerComment = "//////////////////////////////";
            final String headerSpace = "    ";
            final String header = headerSpace + headerComment + " " + headerMessage + " " + headerComment;
            final String header2 =
                    headerSpace + headerComment + " TO REGENERATE RUN GenerateMultiSeries " + headerComment;
            final String header3 =
                    headerSpace + headerComment + " AND THEN RUN GenerateFigureImmutable " + headerComment;

            code.append(String.join("\n", header, header2, header3)).append("\n\n");

            code.append(generateClasses(skip));

            final String axes = Files.lines(Paths.get(outputFile)).reduce("\n", (a, b) -> a + "\n" + b);

            int cutPoint = axes.lastIndexOf(header);

            if (cutPoint != axes.indexOf(header)) {
                throw new IllegalArgumentException(
                        "Input source code contains two autogenerated sections! file=" + outputFile);
            }

            if (cutPoint < 0) {
                cutPoint = axes.lastIndexOf("}");
            } else {
                cutPoint = axes.lastIndexOf("\n", cutPoint);
            }

            String newaxes = axes.substring(0, cutPoint) + "\n" + code + "\n" + "}\n";
            final String newcode = newaxes.trim();


            if (assertNoChange) {
                String oldCode = new String(Files.readAllBytes(Paths.get(outputFile)));
                if (!newcode.equals(oldCode)) {
                    throw new RuntimeException("Change in generated code for " + outputFile
                            + ".  Run GenerateMultiSeries or \"./gradlew :Generators:generateMultiSeries\" to regenerate\n");
                }
            } else {
                PrintWriter out = new PrintWriter(outputFile);
                out.print(newcode);
                out.close();
            }
        }

        private String generateClasses(final Set<Method> skip) throws ClassNotFoundException {

            final StringBuilder code = new StringBuilder();
            if (!isInterface && !isAbstract) {
                code.append(indent(1)).append("@Override public void initializeSeries(")
                        .append(parameterizedType)
                        .append(" series) {\n").append(indent(2)).append("$$initializeSeries$$(series);\n")
                        .append(indent(1)).append("}\n\n");
            }
            final List<GroovyStaticImportGenerator.JavaFunction> sortedMethods = new ArrayList<>();
            final List<GroovyStaticImportGenerator.JavaFunction> methodsWithFunctionParameter = new ArrayList<>();
            for (final String clazz : interfaces) {
                final Class dataseries = Class.forName(clazz);
                final Method[] methods = Arrays.stream(dataseries.getMethods())
                        .filter(m -> !skip.contains(m))
                        .toArray(Method[]::new);

                final GroovyStaticImportGenerator.JavaFunction[] functionalMethods =
                        filterBadMethods(Arrays.stream(methods)
                                .filter(m -> hasFunction(m.getParameterTypes()))
                                .map(GroovyStaticImportGenerator.JavaFunction::new)
                                .toArray(GroovyStaticImportGenerator.JavaFunction[]::new));

                final GroovyStaticImportGenerator.JavaFunction[] nonFunctionalMethods =
                        Arrays.stream(methods)
                                .filter(m -> !hasFunction(m.getParameterTypes()))
                                .map(GroovyStaticImportGenerator.JavaFunction::new)
                                .toArray(GroovyStaticImportGenerator.JavaFunction[]::new);
                Arrays.sort(functionalMethods);
                Arrays.sort(nonFunctionalMethods);
                Collections.addAll(methodsWithFunctionParameter, functionalMethods);
                Collections.addAll(sortedMethods, nonFunctionalMethods);
            }

            final Set<String> methodsDone = new HashSet<>(); // used to avoid duplicates
            for (final GroovyStaticImportGenerator.JavaFunction function : methodsWithFunctionParameter) {
                final String mapName = createMapName(function);
                if (!methodsDone.add(mapName)) {
                    continue;
                }
                final String outputSimple = isTransform ? "AbstractMultiSeries<SERIES>" : output.getSimpleName();
                code.append(createMethodWithFunctionParameter(outputSimple, function));
                code.append("\n\n");
            }

            final Map<String, GroovyStaticImportGenerator.JavaFunction> mapToFunction = new HashMap<>();
            for (final GroovyStaticImportGenerator.JavaFunction function : sortedMethods) {
                final String mapName = createMapName(function);
                if (mapToFunction.get(mapName) != null) {
                    continue;
                }
                mapToFunction.put(mapName, function);
                final String outputSimple = isTransform ? "AbstractMultiSeries<SERIES>" : output.getSimpleName();
                if (!isInterface && !isAbstract && !isTransform) {
                    String mapType = getMapType(function);
                    code.append(createMap(mapType, mapName));
                    code.append(createGetter(mapType, mapName));
                }
                code.append(createMethod(outputSimple, function, mapName));
                code.append("\n\n");
            }


            if (!isAbstract && !isInterface) {
                code.append(createInitializeFunction(mapToFunction));
            }

            if (!isAbstract && !isInterface && !isTransform) {
                code.append(createCopyConstructor(mapToFunction.keySet()));
            }

            return code.toString();
        }

        private boolean hasFunction(Class<?>[] parameterTypes) {
            for (final Class<?> parameter : parameterTypes) {
                if (parameter.equals(Function.class) || parameter.equals(Closure.class)) {
                    return true;
                }
            }

            return false;
        }

        private String createCopyConstructor(final Set<String> strings) {
            final List<String> copiedVars = new ArrayList<>();
            for (final String var : strings) {
                copiedVars.add("        %NEWSERIES%.%VAR% = %VAR%.copy();".replaceAll("%VAR%", var));
            }
            final String ret = "    @Override\n" +
                    "    public %CLASSNAME% copy(AxesImpl axes) {\n" +
                    "        final %CLASSNAME% %NEWSERIES% = new %CLASSNAME%(this, axes);\n" +
                    "        " + String.join("\n", copiedVars) + "\n" +
                    "        return %NEWSERIES%;\n" +
                    "    }";

            return ret.replaceAll("%CLASSNAME%", output.getSimpleName()).replaceAll("%NEWSERIES%", "__s__");
        }

        private String createInitializeFunction(
                final Map<String, GroovyStaticImportGenerator.JavaFunction> mapToFunction) {
            final StringBuilder code = new StringBuilder();
            final Map<GroovyStaticImportGenerator.JavaFunction, Function<String, String>> functionToGenerics =
                    createFunctionToGenerics(mapToFunction.values());

            code.append(indent(1)).append("@SuppressWarnings(\"unchecked\") \n").append(indent(1)).append("private ")
                    .append(createGenericInitializeSeries(mapToFunction, functionToGenerics))
                    .append("void")
                    .append(" $$initializeSeries$$(")
                    .append(parameterizedType)
                    .append(" series) {\n");
            if (isTransform) {
                return code.append(createInitializeFunctionTransform()).toString();
            }

            code.append(indent(2)).append("String name = series.name().toString();\n");

            boolean objectArrayInitialized = false;
            int numConsumers = 0;
            for (final Map.Entry<String, GroovyStaticImportGenerator.JavaFunction> entry : mapToFunction.entrySet()) {
                final String map = entry.getKey();
                final GroovyStaticImportGenerator.JavaFunction function = entry.getValue();
                final boolean oneArg = function.getParameterNames().length == 1;

                if (oneArg) {
                    final String consumerName = "consumer" + numConsumers++;
                    final Function<String, String> toGenerics = functionToGenerics.get(function);
                    try {
                        String typeName = function.getParameterTypes()[0].getTypeName();
                        typeName = typeName.contains(Closure.class.getCanonicalName())
                                ? ClosureFunction.class.getCanonicalName()
                                : typeName;
                        code.append(indent(2)).append("java.util.function.Consumer<")
                                .append(toGenerics == null
                                        ? TypeUtils.getBoxedType(ClassUtil.lookupClass(typeName)).getCanonicalName()
                                        : toGenerics.apply(typeName))
                                .append("> ").append(consumerName)
                                .append(" = series::")
                                .append(function.getMethodName());
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    code.append(";\n");
                    code.append(indent(2))
                            .append(map)
                            .append(".runIfKeyExistsCast(")
                            .append(consumerName)
                            .append(", name);\n");
                } else if (!objectArrayInitialized) {
                    code.append(indent(2)).append("java.lang.Object[] ");
                    objectArrayInitialized = true;
                }

                if (!oneArg) {
                    code.append(indent(2)).append("objectArray = ")
                            .append(map)
                            .append(".get(name);\n");
                    code.append(indent(2)).append("if(objectArray != null) {series.")
                            .append(function.getMethodName())
                            .append("(");

                    final Type[] types = function.getParameterTypes();
                    final List<String> args = new ArrayList<>();
                    for (int i = 0; i < types.length; i++) {
                        String typeName = types[i].getTypeName();

                        if (typeName.contains("java.util.Map") || typeName.contains("groovy.lang.Closure")
                                || typeName.contains("java.util.function.Function")) {
                            typeName = typeName.replace(Closure.class.getCanonicalName(),
                                    ClosureFunction.class.getCanonicalName());
                            final int ind = typeName.indexOf("<");
                            typeName = typeName.substring(0, ind < 0 ? typeName.length() : ind);
                        }

                        final String arg;
                        if (typeName.equals("io.deephaven.db.tables.Table")) {
                            arg = "((io.deephaven.db.plot.util.tables.TableHandle) objectArray[" + i + "]).getTable()";
                        } else if (typeName.equals("java.lang.Object")) {
                            arg = "objectArray[" + i + "]";
                        } else {
                            arg = "(" + typeName + ") objectArray[" + i + "]";
                        }

                        args.add(arg);
                    }
                    final String cast = String.join(", ", args);
                    final Function<String, String> f = functionToGenerics.get(function);
                    code.append(f == null ? cast : f.apply(cast)).append(");}\n\n");
                }
            }
            return code.append("\n").append(indent(1)).append("}\n").toString();
        }

        private String createInitializeFunctionTransform() {
            return indent(2) + "this.series.initializeSeries((SERIES2) series);\n" + indent(1) + "}\n";
        }

        private Map<GroovyStaticImportGenerator.JavaFunction, Function<String, String>> createFunctionToGenerics(
                Collection<GroovyStaticImportGenerator.JavaFunction> functionSet) {
            final Map<GroovyStaticImportGenerator.JavaFunction, Function<String, String>> map = new HashMap<>();
            final Map<String, Type> generics = new HashMap<>();
            final Map<String, Integer> counter = new HashMap<>();
            for (final GroovyStaticImportGenerator.JavaFunction function : functionSet) {
                for (TypeVariable<Method> typeVariable : function.getTypeParameters()) {
                    final String typeName = typeVariable.getTypeName();
                    final Type[] bounds = typeVariable.getBounds();
                    if (bounds.length > 1) { // probably don't need more than one extend at this point, this is just
                                             // much easier right now todo?
                        throw new UnsupportedOperationException("Doesn't support more than one type right now.");
                    }
                    final Type type = typeVariable.getBounds()[0];
                    if (!generics.containsKey(typeName)) {
                        generics.put(typeName, type);
                    } else if (!generics.get(typeName).equals(type)) {
                        final int count = counter.getOrDefault(typeName, 0);
                        final String newTypeName = typeName + count;
                        counter.put(typeName, count + 1);
                        generics.put(newTypeName, type);

                        final Function<String, String> oldFunction = map.get(function);
                        final Function<String, String> newFunction;
                        if (oldFunction == null) {
                            newFunction = s -> s.replaceAll(typeName, newTypeName);
                        } else {
                            newFunction = s -> oldFunction.apply(s).replaceAll(typeName, newTypeName);
                        }
                        map.put(function, newFunction);
                    }
                }
            }
            return map;
        }

        private String createGenericInitializeSeries(
                final Map<String, GroovyStaticImportGenerator.JavaFunction> mapToFunction,
                final Map<GroovyStaticImportGenerator.JavaFunction, Function<String, String>> functionToGenerics) {
            final List<String> args = new ArrayList<>();
            final Set<String> variableNames = new HashSet<>();
            for (final GroovyStaticImportGenerator.JavaFunction function : mapToFunction.values()) {
                for (final TypeVariable<Method> typeVariable : function.getTypeParameters()) {
                    String genericTypes = getGenericTypes(typeVariable);
                    if (!genericTypes.isEmpty()) {
                        final Function<String, String> f = functionToGenerics.get(function);
                        genericTypes = f == null ? genericTypes : f.apply(genericTypes);
                        final int ind = genericTypes.indexOf(" ");
                        final String name = ind > 0 ? genericTypes.substring(0, ind) : genericTypes;
                        if (variableNames.add(name)) {
                            args.add(genericTypes);
                        }
                    }
                }
            }
            return args.isEmpty() ? "" : "<" + String.join(", ", args) + "> ";
        }

        private String createMapName(final GroovyStaticImportGenerator.JavaFunction function) {
            return "%METHOD%SeriesNameTo%TYPES%Map".replaceAll("%TYPES%", createArgsString(function))
                    .replaceAll("%METHOD%", function.getMethodName());
        }

        private String createMap(String mapType, final String mapName) {
            return indent(1) + "private " + mapType + " " + mapName + " = new io.deephaven.db.plot.util.PlotUtils" +
                    ".HashMapWithDefault<>();\n";
        }

        private String createMethod(final String returnClass, final GroovyStaticImportGenerator.JavaFunction function,
                final String mapName) {
            final StringBuilder code = new StringBuilder();
            code.append(createMethodHeader(returnClass, function));
            if (isTransform) {
                code.append(createTransformBody(function));
            } else if (isAbstract && !isInterface) {
                code.append(createExceptionMethodBody(function));
            } else if (!isInterface) {
                code.append(createMapCode(function, mapName));
            }

            return code.toString();
        }

        private String createGetter(String mapType, String mapName) {
            return indent(1) + "public " + mapType + " " + mapName + "() {\n" +
                    indent(2) + "return " + mapName + ";\n" +
                    indent(1) + "}\n";
        }

        private String createMethodWithFunctionParameter(String returnClass,
                GroovyStaticImportGenerator.JavaFunction function) throws ClassNotFoundException {
            final StringBuilder code = new StringBuilder();
            code.append(createMethodHeader(returnClass, function));
            if (isTransform) {
                code.append(createTransformBody(function));
            } else if (isAbstract && !isInterface) {
                code.append(createExceptionMethodBody(function));
            } else if (!isInterface) {
                code.append(createFunctionalBody(returnClass, function));
            }

            return code.toString();
        }

        private String createFunctionalBody(final String returnClass, GroovyStaticImportGenerator.JavaFunction function)
                throws ClassNotFoundException {
            if (function.getParameterTypes()[0].getTypeName().startsWith("groovy.lang.Closure")) {
                return indent(2) + "return " + function.getMethodName()
                        + "(new io.deephaven.db.plot.util.functions.ClosureFunction<>("
                        + function.getParameterNames()[0] + "), keys);\n" + indent(1) + "}\n\n";
            }

            final String tableMethodName = getTableMethodName(function.getMethodName());
            return indent(2) + "final String newColumn = " + getColumnNameConstant(function.getMethodName())
                    + " + this.hashCode();\n" +
                    indent(2) + "applyFunction(" + function.getParameterNames()[0] + ", newColumn, "
                    + getFunctionInput(function) + ", " + getReturnTypeName(function) + ".class);\n" +
                    indent(2)
                    + "chart().figure().registerFigureFunction(new io.deephaven.db.plot.util.functions.FigureImplFunction(f -> f."
                    +
                    tableMethodName +
                    "(" + getFigureFunctionInput(returnClass, function, tableMethodName)
                    + "));\n" +
                    indent(2) + "return this;\n" + indent(1) + "}\n\n";
        }

        private String getFigureFunctionInput(final String returnClass,
                final GroovyStaticImportGenerator.JavaFunction function, final String tableMethodName)
                throws ClassNotFoundException {
            final StringBuilder code = new StringBuilder();
            code.append(isSwappable ? "new SelectableDataSetSwappableTable(getSwappableTable()), "
                    : "getTableMapHandle().getTable(), ");

            if (function.getMethodName().equals("pointColorByY")) {
                final Class c = Class.forName("io.deephaven.db.plot.datasets.multiseries." + returnClass);
                final Method[] methods = Arrays.stream(c.getDeclaredMethods())
                        .filter(m -> m.getName().equals(tableMethodName))
                        .filter(m -> m.getParameterTypes().length > 0 && m.getParameterTypes()[0].equals(Table.class))
                        .toArray(Method[]::new);

                Arrays.sort(methods, Comparator.comparingInt(Method::getParameterCount));

                Method tableMethod = methods[methods.length - 1];
                switch (tableMethod.getParameterCount()) {
                    case 3: // table, values, keys -> xy dataset
                        code.append("newColumn");
                        break;
                    case 4: // table, key, values, keys -> category dataset
                        code.append("getX(), newColumn");
                        break;
                    default:
                        log.warning(tableMethod.toString());
                        throw new IllegalStateException("Can not calculate function input for function "
                                + tableMethodName + " in class " + function.getClassNameShort());
                }

                return code.append(", keys), this").toString();
            }

            final Class c = Class.forName(function.getClassName());
            final Method[] methods = Arrays.stream(c.getMethods())
                    .filter(m -> m.getName().equals(tableMethodName))
                    .filter(m -> m.getParameterTypes().length > 0 && m.getParameterTypes()[0].equals(Table.class))
                    .toArray(Method[]::new);

            if (methods.length < 1) {
                throw new IllegalStateException(
                        "No table methods for method " + tableMethodName + " in class " + function.getClassNameShort());
            }

            if (methods.length != 1) {
                log.warning("More than 1 possible table method for function input for function " + tableMethodName
                        + " in class " + function.getClassNameShort());
                log.warning(Arrays.toString(methods));
                throw new IllegalStateException("Can not calculate function input for function " + tableMethodName
                        + " in class " + function.getClassNameShort());
            }

            final Method method = methods[0];
            switch (method.getParameterCount()) {
                case 2:
                    break;
                case 3:
                    code.append("getX(), ");
                    break;
                default:
                    throw new IllegalStateException("Can not calculate function input for function " + tableMethodName
                            + " in class " + function.getClassNameShort());
            }

            code.append("newColumn, keys), this");

            return code.toString();
        }

        private static GroovyStaticImportGenerator.JavaFunction[] filterBadMethods(
                final GroovyStaticImportGenerator.JavaFunction[] functions) {
            final List<GroovyStaticImportGenerator.JavaFunction> retList = new ArrayList<>();

            final Map<Pair<String, Integer>, GroovyStaticImportGenerator.JavaFunction> functionMap = new HashMap<>();

            for (final GroovyStaticImportGenerator.JavaFunction function : functions) {
                if (function.getParameterTypes()[0].getTypeName().contains("Function")) {
                    final Pair<String, Integer> methodPair =
                            new Pair<>(function.getMethodName(), function.getParameterNames().length);
                    final GroovyStaticImportGenerator.JavaFunction oldFunction = functionMap.get(methodPair);

                    if (oldFunction != null) {
                        if (oldFunction.getTypeParameters().length < 1 && function.getTypeParameters().length > 0) {
                            functionMap.put(methodPair, function);
                        }
                    } else {
                        functionMap.put(methodPair, function);
                    }

                } else {
                    retList.add(function);
                }
            }

            retList.addAll(functionMap.values());
            return retList.toArray(new GroovyStaticImportGenerator.JavaFunction[0]);
        }

        private String getFunctionInput(GroovyStaticImportGenerator.JavaFunction function) {
            if (function.getMethodName().endsWith("ByX")) {
                return "getX()";
            } else if (function.getMethodName().endsWith("ByY")) {
                return "getY()";
            } else if (function.getParameterTypes()[0].getTypeName().startsWith("java.util.function.Function")) {
                return "getX()";
            }

            throw new IllegalStateException("Don't know what to make function input for method "
                    + function.getMethodName() + " param class " + function.getParameterTypes()[0].getClass());
        }

        private String getReturnTypeName(final GroovyStaticImportGenerator.JavaFunction function) {
            if (function.getTypeParameters().length < 1) { // non generics
                final String[] params = function.getParameterTypes()[0].getTypeName().split(",");
                final String returnType = params[params.length - 1];
                return returnType.substring(0, returnType.length() - 1).trim();

            }
            return function.getTypeParameters()[function.getTypeParameters().length - 1].getBounds()[0].getTypeName();
        }


        private String getTableMethodName(String methodName) {
            methodName = methodName.endsWith("ByX") || methodName.endsWith("ByY")
                    ? methodName.substring(0, methodName.length() - 3)
                    : methodName;

            if (methodName.startsWith("pointColor")) {
                return "pointColor";
            } else {
                return methodName;
            }
        }

        private String getColumnNameConstant(String methodName) {
            methodName = methodName.endsWith("ByX") || methodName.endsWith("ByY")
                    ? methodName.substring(0, methodName.length() - 3)
                    : methodName;

            if (methodName.startsWith("pointColor")) {
                return "io.deephaven.db.plot.datasets.ColumnNameConstants.POINT_COLOR";
            } else if (methodName.equals("pointSize")) {
                return "io.deephaven.db.plot.datasets.ColumnNameConstants.POINT_SIZE";
            } else if (methodName.equals("pointShape")) {
                return "io.deephaven.db.plot.datasets.ColumnNameConstants.POINT_SHAPE";
            } else if (methodName.equals("pointLabel")) {
                return "io.deephaven.db.plot.datasets.ColumnNameConstants.POINT_LABEL";
            }

            throw new IllegalStateException("No column name constant corresponds to method name " + methodName);
        }

        private String createTransformBody(final GroovyStaticImportGenerator.JavaFunction function) {
            final List<String> args = new ArrayList<>();
            Collections.addAll(args, function.getParameterNames());
            args.add("keys");
            return indent(2) + "//noinspection unchecked\n" + indent(2) + "return (AbstractMultiSeries<SERIES>) series."
                    + function.getMethodName() + "(" + String.join(", ", args) + ");\n" + indent(1) + "}\n\n";
        }

        private String createExceptionMethodBody(final GroovyStaticImportGenerator.JavaFunction function) {
            return indent(2)
                    + "throw new PlotUnsupportedOperationException(\"DataSeries \" + this.getClass() + \" does not support method "
                    + function.getMethodName() + " for arguments " + Arrays.toString(function.getParameterTypes())
                    + ". If you think this method should work, try placing your keys into an Object array\", this);\n"
                    + indent(1) + "}\n\n";
        }

        private String createMethodHeader(final String returnClass,
                final GroovyStaticImportGenerator.JavaFunction function) {
            String methodHeader = indent(1) + (!isInterface ? "@Override public " : "") + getGenericTypes(function)
                    + returnClass + (isGeneric ? "<SERIES>" : "") + " " + function.getMethodName() + "(";

            final String[] args = function.getParameterNames();
            final Type[] types = function.getParameterTypes();

            if (args.length != types.length) {
                throw new IllegalStateException("Number of parameter names and number of parameter types not equal!");
            }

            final List<String> arguments = new ArrayList<>();
            for (int i = 0; i < args.length; i++) {
                arguments.add("final " + types[i].getTypeName() + " " + args[i]);
            }

            return methodHeader + String.join(", ", arguments) + ", final Object... keys)"
                    + (!isInterface ? " {\n" : ";\n");
        }

        private String getGenericTypes(final GroovyStaticImportGenerator.JavaFunction function) {
            final TypeVariable<Method>[] typeParameters = function.getTypeParameters();
            if (typeParameters.length == 0) {
                return "";
            }

            final List<String> params = new ArrayList<>();
            for (TypeVariable<Method> typeVariable : typeParameters) {
                params.add(getGenericTypes(typeVariable));
            }
            return "<" + String.join(", ", params) + "> ";
        }

        private String getGenericTypes(TypeVariable<Method> typeVariable) {
            final List<String> bounds = new ArrayList<>();
            for (final Type bound : typeVariable.getBounds()) {
                final String typeName = bound.getTypeName();
                if (!typeName.equals("java.lang.Object")) {
                    bounds.add(bound.getTypeName());
                }
            }
            return typeVariable.getName() + (bounds.isEmpty() ? "" : " extends " + String.join(" & ", bounds));
        }

        private String createMapCode(final GroovyStaticImportGenerator.JavaFunction function, final String mapName) {
            final Type[] vars = function.getParameterTypes();
            final String[] names = function.getParameterNames();
            final StringBuilder code = new StringBuilder();
            for (int i = 0; i < vars.length; i++) {
                if (vars[i].getTypeName().contains(Closure.class.getCanonicalName())) {
                    code.append(indent(2))
                            .append(ClosureFunction.class.getCanonicalName())
                            .append(" ").append(names[i])
                            .append("ClosureFunction = new ")
                            .append(ClosureFunction.class.getCanonicalName())
                            .append("<>(")
                            .append(names[i])
                            .append(");\n");
                }
            }
            final Map<String, String> tableToTableHandleVarMap = new HashMap<>();
            for (int i = 0; i < vars.length; i++) {
                final Type var = vars[i];
                if (var.getTypeName().equals("io.deephaven.db.tables.Table")) {
                    final String handleName = names[i] + "Handle";
                    tableToTableHandleVarMap.put(names[i], handleName);
                    code.append(indent(1)).append("final io.deephaven.db.plot.util.tables.TableHandle ")
                            .append(handleName).append(" = new io.deephaven.db.plot.util.tables.TableHandle(")
                            .append(names[i]);

                    for (int j = i + 1; j < vars.length; j++) {
                        if (vars[j].getTypeName().equals("java.lang.String")) {
                            code.append(", ").append(names[j]);
                        } else {
                            break;
                        }
                    }
                    code.append(");\n");
                    code.append(indent(1)).append("addTableHandle(").append(handleName).append(");\n");
                }
            }

            final String args = createSmartKeyArgs(function, tableToTableHandleVarMap);
            final boolean oneArgument = function.getParameterNames().length == 1;

            code.append(indent(2)).append("if(keys == null || keys.length == 0) {\n").append(indent(3))
                    .append(mapName)
                    .append(".setDefault(")
                    .append(oneArgument ? args : "new Object[]{" + args + "}")
                    .append(");\n")
                    .append(indent(2)).append("} else {");
            code.append("\n").append(indent(3))
                    .append(mapName)
                    .append(".put(namingFunction.apply(keys.length == 1 ? keys[0] : new io.deephaven.datastructures.util.SmartKey(keys)), ");
            if (oneArgument) {
                code.append("\n").append(indent(4)).append(args).append(");\n").append(indent(2)).append("}\n");
            } else {
                code.append("\n").append(indent(4)).append("new Object[]{ ").append(args).append("});\n")
                        .append(indent(2)).append("}\n");
            }

            return code.append("\n").append(indent(2)).append("return this;\n").append(indent(1)).append("}\n\n")
                    .toString();
        }

        private String createSmartKeyArgs(final GroovyStaticImportGenerator.JavaFunction function,
                final Map<String, String> tableToTableHandleVarMap) {
            List<String> args = new ArrayList<>();
            final Type[] vars = function.getParameterTypes();
            final String[] names = Arrays.stream(function.getParameterNames())
                    .map(s -> tableToTableHandleVarMap.getOrDefault(s, s)).toArray(String[]::new);

            for (int i = 0; i < vars.length; i++) {
                if (vars[i].getTypeName().equals("java.lang.Object[]")) {
                    args.add("new Object[]{" + names[i] + "}");
                } else if (vars[i].getTypeName().contains(Closure.class.getCanonicalName())) {
                    args.add(names[i] + "ClosureFunction");
                } else {
                    args.add(names[i]);
                }
            }

            return String.join(", ", args);
        }

        private String createArgsString(final GroovyStaticImportGenerator.JavaFunction function) {
            String args = "";
            for (final Type type : function.getParameterTypes()) {
                String typeName = type.getTypeName();
                final int indCarrot = typeName.indexOf("<");
                typeName = typeName.substring(0, indCarrot > 0 ? indCarrot : typeName.length());
                final int indDot = typeName.lastIndexOf(".");
                args += typeName.substring(indDot + 1, typeName.length()).replaceAll("\\[", "Array").replaceAll("\\]",
                        "");
            }
            return args;
        }
    }

    private static String getMapType(GroovyStaticImportGenerator.JavaFunction function) {
        final String className;
        final Type[] types = function.getParameterTypes();
        if (types.length == 1) {
            Class possiblyBoxed;
            try {
                possiblyBoxed = TypeUtils.getBoxedType(ClassUtil.lookupClass(types[0].getTypeName()));
            } catch (ClassNotFoundException e) {
                possiblyBoxed = Object.class;
            }
            if (possiblyBoxed.getCanonicalName().equals(Closure.class.getCanonicalName())) {
                className = ClosureFunction.class.getCanonicalName();
            } else {
                className = possiblyBoxed.getCanonicalName();
            }
        } else {
            className = "java.lang.Object[]";
        }
        return "io.deephaven.db.plot.util.PlotUtils.HashMapWithDefault<String, " + className + ">";
    }

}
