/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.plot.*;
import io.deephaven.plot.datasets.DataSeries;
import io.deephaven.plot.datasets.multiseries.MultiSeries;
import io.deephaven.plot.errors.PlotExceptionCause;
import io.deephaven.libs.GroovyStaticImportGenerator;
import io.deephaven.libs.GroovyStaticImportGenerator.JavaFunction;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.deephaven.plot.util.PlotGeneratorUtils.indent;

/**
 * Creates a functional interface for plotting.
 */
@SuppressWarnings("StringConcatenationInLoop")
public class GenerateFigureImmutable {
    // See also GroovyStaticImportGenerator

    private static Logger log = Logger.getLogger(GenerateFigureImmutable.class.toString());

    private static final String CLASS_NAME_INTERFACE = "io.deephaven.plot.Figure";
    private static final String CLASS_NAME_IMPLEMENTATION = "io.deephaven.plot.FigureImpl";

    private final String outputClass;
    private final String outputClassNameShort;
    private boolean isInterface;
    private final String[] imports;
    private final String[] interfaces;
    private final String[] seriesInterfaces;
    private final Map<String, TreeSet<JavaFunction>> seriesSignatureGroups;
    private final Map<JavaFunction, JavaFunction> nonstaticFunctions = new TreeMap<>();
    private final Collection<Predicate<JavaFunction>> skips;
    private final Function<JavaFunction, String> functionNamer;

    private GenerateFigureImmutable(final boolean isInterface, final String[] imports, final String[] interfaces,
            final String[] seriesInterfaces,
            final Collection<Predicate<JavaFunction>> skips, final Function<JavaFunction, String> functionNamer)
            throws ClassNotFoundException {
        this.outputClass = isInterface ? CLASS_NAME_INTERFACE : CLASS_NAME_IMPLEMENTATION;
        this.isInterface = isInterface;
        this.outputClassNameShort = this.outputClass.substring(outputClass.lastIndexOf('.') + 1);
        this.imports = imports;
        this.interfaces = interfaces;
        this.seriesInterfaces = seriesInterfaces;
        this.seriesSignatureGroups = commonSignatureGroups(seriesInterfaces);
        this.skips = skips;
        this.functionNamer = functionNamer == null ? JavaFunction::getMethodName : functionNamer;

        for (final String imp : interfaces) {
            final Class<?> c = Class.forName(imp);
            log.info("Processing class: " + c);

            for (final Method m : c.getMethods()) {
                log.info("Processing method (" + c + "): " + m);
                boolean isStatic = Modifier.isStatic(m.getModifiers());
                boolean isPublic = Modifier.isPublic(m.getModifiers());
                boolean isObject = m.getDeclaringClass().equals(Object.class);

                if (!isStatic && isPublic && !isObject) {
                    addPublicNonStatic(m);
                }
            }
        }
    }


    private boolean skip(final JavaFunction f) {
        boolean skip = false;
        for (Predicate<JavaFunction> skipCheck : skips) {
            skip = skip || skipCheck.test(f);
        }

        return skip;
    }


    private JavaFunction signature(final JavaFunction f) {
        return new JavaFunction(
                outputClass,
                outputClassNameShort,
                functionNamer.apply(f),
                f.getTypeParameters(),
                f.getReturnType(),
                f.getParameterTypes(),
                f.getParameterNames(),
                f.isVarArgs());
    }

    private void addPublicNonStatic(Method m) {
        log.info("Processing public non-static method: " + m);

        final JavaFunction f = new JavaFunction(m);
        final JavaFunction signature = signature(f);

        boolean skip = skip(f);

        if (skip) {
            log.warning("*** Skipping function: " + f);
            return;
        }

        if (nonstaticFunctions.containsKey(signature)) {
            JavaFunction fAlready = nonstaticFunctions.get(signature);
            final String message = "Signature Already Present:	" + fAlready + "\t" + signature;
            log.severe(message);
            throw new RuntimeException(message);
        } else {
            log.info("Added public method: " + f);
            nonstaticFunctions.put(signature, f);
        }
    }

    private Set<String> generateImports() {
        Set<String> imports = new TreeSet<>();

        if (isInterface) {
            return imports;
        }

        imports.addAll(Arrays.asList(this.imports));

        final ArrayList<JavaFunction> functions = new ArrayList<>();
        functions.addAll(nonstaticFunctions.values());

        for (JavaFunction f : functions) {
            imports.add(f.getClassName());

            imports.addAll(typesToImport(f.getReturnType()));

            for (Type t : f.getParameterTypes()) {
                imports.addAll(typesToImport(t));
            }
        }

        for (TreeSet<JavaFunction> fs : this.seriesSignatureGroups.values()) {
            for (JavaFunction f : fs) {
                imports.add(f.getClassName());
            }
        }

        return imports;
    }

    private static Set<String> typesToImport(Type t) {
        Set<String> result = new LinkedHashSet<>();

        if (t instanceof Class) {
            final Class<?> c = (Class) t;
            final boolean isArray = c.isArray();
            final boolean isPrimitive = c.isPrimitive();

            if (isPrimitive) {
                return result;
            } else if (isArray) {
                return typesToImport(c.getComponentType());
            } else {
                result.add(t.getTypeName());
            }
        } else if (t instanceof ParameterizedType) {
            final ParameterizedType pt = (ParameterizedType) t;
            result.add(pt.getRawType().getTypeName());

            for (Type a : pt.getActualTypeArguments()) {
                result.addAll(typesToImport(a));
            }
        } else if (t instanceof TypeVariable) {
            // type variables are generic so they don't need importing
            return result;
        } else if (t instanceof GenericArrayType) {
            GenericArrayType at = (GenericArrayType) t;
            return typesToImport(at.getGenericComponentType());
        } else {
            throw new UnsupportedOperationException("Unsupported Type type: " + t.getClass());
        }

        return result;
    }

    private String generateImplements() {
        final StringBuilder sb = new StringBuilder();

        if (isInterface) {
            sb.append(" extends ");
            sb.append("java.io.Serializable");

            for (final String[] ii : new String[][] {interfaces, seriesInterfaces}) {
                for (final String iface : ii) {
                    // final String[] siface = iface.split("[.]");
                    // final String name = siface[siface.length - 1];
                    sb.append(", ").append(iface);
                }
            }
        } else {
            sb.append(" implements " + CLASS_NAME_INTERFACE);
        }

        return sb.toString();
    }

    private String generateCode() {

        String code = "/*\n" +
                " * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending\n" +
                " */\n\n" +
                "/****************************************************************************************************************************\n"
                +
                " ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - Run GenerateFigureImmutable or \"./gradlew :Generators:generateFigureImmutable\" to regenerate\n"
                +
                " ****************************************************************************************************************************/\n\n";

        code += "package io.deephaven.plot;\n\n";

        Set<String> imports = generateImports();

        for (String imp : imports) {
            code += "import " + imp.replace("$", ".") + ";\n";
        }

        code += "\n";
        code += "/** An interface for constructing plots.  A Figure is immutable, and all function calls return a new immutable Figure instance.";
        code += "*/\n";
        code += "@SuppressWarnings({\"unused\", \"RedundantCast\", \"SameParameterValue\"})\n";
        code += "public" + (isInterface ? " interface " : " class ") + outputClassNameShort + generateImplements()
                + " {\n";

        code += "\n";
        code += createConstructor();
        code += "\n";
        code += createFigureFuncs();
        code += "\n";

        for (final JavaFunction f : nonstaticFunctions.values()) {
            final boolean skip = skip(f);

            if (skip) {
                log.warning("*** Skipping function: " + f);
                continue;
            }

            final String s = createFunction(f);

            code += s;
            code += "\n";
        }

        for (Map.Entry<String, TreeSet<JavaFunction>> e : seriesSignatureGroups.entrySet()) {
            final JavaFunction f = e.getValue().first();
            final boolean skip = skip(f);

            if (skip) {
                log.warning("*** Skipping function: " + f);
                continue;
            }

            for (JavaFunction jf : this.nonstaticFunctions.values()) {
                if (e.getKey().equals(createFunctionSignature(jf))) {
                    throw new IllegalStateException("Repeated function signature. " + e.getKey());
                }
            }

            final String s = createSignatureGroupFunction(e.getValue());

            code += s;
            code += "\n";
        }

        code += "}\n\n";

        return code;
    }

    private String createConstructor() {
        if (isInterface) {
            return "";
        }

        return "    private static final long serialVersionUID = -4519904656095275663L;\n" +
                "\n" +
                "    private final BaseFigureImpl figure;\n" +
                "    private final ChartLocation lastChart;\n" +
                "    private final AxesLocation lastAxes;\n" +
                "    private final AxisLocation lastAxis;\n" +
                "    private final SeriesLocation lastSeries;\n" +
                "    private final Map<ChartLocation, AxesLocation> lastAxesMap;\n" +
                "    private final Map<AxesLocation, AxisLocation> lastAxisMap;\n" +
                "    private final Map<AxesLocation, SeriesLocation> lastSeriesMap;\n" +
                "\n" +
                "    private " + outputClassNameShort
                + "(final BaseFigureImpl figure, final ChartLocation lastChart, final AxesLocation lastAxes, final AxisLocation lastAxis, final SeriesLocation lastSeries, final Map<ChartLocation, AxesLocation> lastAxesMap, final Map<AxesLocation, AxisLocation> lastAxisMap, final Map<AxesLocation, SeriesLocation> lastSeriesMap) {\n"
                +
                "        this.figure = Require.neqNull(figure, \"figure\");\n" +
                "        this.lastChart = lastChart;\n" +
                "        this.lastAxes = lastAxes;\n" +
                "        this.lastAxis = lastAxis;\n" +
                "        this.lastSeries = lastSeries;\n" +
                "        this.lastAxesMap = new HashMap<>(lastAxesMap);\n" +
                "        this.lastAxisMap = new HashMap<>(lastAxisMap);\n" +
                "        this.lastSeriesMap = new HashMap<>(lastSeriesMap);\n" +
                "        if(this.lastAxes != null) { this.lastAxesMap.put(this.lastChart, this.lastAxes); }\n" +
                "        if(this.lastAxis != null) { this.lastAxisMap.put(this.lastAxes, this.lastAxis); }\n" +
                "        if(this.lastSeries != null) { this.lastSeriesMap.put(this.lastAxes, this.lastSeries); }\n" +
                "    }\n" +
                "\n" +
                "    public " + outputClassNameShort + "(final " + outputClassNameShort + " figure) {\n" +
                "        this.figure = Require.neqNull(figure, \"figure\").figure;\n" +
                "        this.lastChart = figure.lastChart;\n" +
                "        this.lastAxes = figure.lastAxes;\n" +
                "        this.lastAxis = figure.lastAxis;\n" +
                "        this.lastSeries = figure.lastSeries;\n" +
                "        this.lastAxesMap = figure.lastAxesMap;\n" +
                "        this.lastAxisMap = figure.lastAxisMap;\n" +
                "        this.lastSeriesMap = figure.lastSeriesMap;\n" +
                "    }\n" +
                "\n" +
                "    private " + outputClassNameShort + "(final BaseFigureImpl figure) {\n" +
                "        this(figure,null,null,null,null,new HashMap<>(),new HashMap<>(),new HashMap<>());\n" +
                "    }\n" +
                "\n" +
                "    " + outputClassNameShort + "() {\n" +
                "        this(new BaseFigureImpl());\n" +
                "    }\n" +
                "\n" +
                "    " + outputClassNameShort + "(final int numRows, final int numCols) {\n" +
                "        this(new BaseFigureImpl(numRows,numCols));\n" +
                "    }\n" +
                "\n" +
                "    private AxesLocation resolveLastAxes(final BaseFigureImpl figure, final ChartLocation chartLoc){\n"
                +
                "        if(chartLoc == null){\n" +
                "            return null;\n" +
                "        }\n" +
                "\n" +
                "        final AxesLocation a0 = lastAxesMap.get(chartLoc);\n" +
                "\n" +
                "        if( a0 != null) {\n" +
                "            return a0;\n" +
                "        }\n" +
                "\n" +
                "        final List<AxesImpl> axs = chartLoc.get(figure).getAxes();\n" +
                "        return axs.isEmpty() ? null : new AxesLocation(axs.get(axs.size()-1));\n" +
                "    }\n" +
                "\n" +
                "    private AxisLocation resolveLastAxis(final BaseFigureImpl figure, final AxesLocation axesLoc){\n" +
                "        if(axesLoc == null){\n" +
                "            return null;\n" +
                "        }\n" +
                "\n" +
                "        final AxisLocation a0 = lastAxisMap.get(axesLoc);\n" +
                "\n" +
                "        if( a0 != null ){\n" +
                "            return a0;\n" +
                "        }\n" +
                "\n" +
                "        final AxesImpl axs = axesLoc.get(figure);\n" +
                "        return axs.dimension() <= 0 ? null : new AxisLocation(axs.axis(axs.dimension()-1));\n" +
                "    }\n" +
                "\n" +
                "    private SeriesLocation resolveLastSeries(final BaseFigureImpl figure, final AxesLocation axesLoc){\n"
                +
                "        if(axesLoc == null){\n" +
                "            return null;\n" +
                "        }\n" +
                "\n" +
                "        final SeriesLocation s0 = lastSeriesMap.get(axesLoc);\n" +
                "\n" +
                "        if( s0 != null ){\n" +
                "            return s0;\n" +
                "        }\n" +
                "\n" +
                "        final SeriesInternal s1 = axesLoc.get(figure).dataSeries().lastSeries();\n" +
                "        return s1 == null ? null : new SeriesLocation(s1);\n" +
                "    }\n" +
                "\n" +
                "\n" +
                "    /**\n" +
                "     * Gets the mutable figure backing this immutable figure.\n" +
                "     *\n" +
                "     * @return mutable figure backing this immutable figure\n" +
                "     */\n" +
                "    public BaseFigureImpl getFigure() { return this.figure; }\n" +
                "\n" +
                "\n" +
                "    private " + outputClassNameShort + " make(final BaseFigureImpl figure){\n" +
                "        final ChartLocation chartLoc = this.lastChart;\n" +
                "        final AxesLocation axesLoc = this.lastAxes;\n" +
                "        final AxisLocation axisLoc = this.lastAxis;\n" +
                "        final SeriesLocation seriesLoc = this.lastSeries;\n" +
                "        return new " + outputClassNameShort
                + "(figure, chartLoc, axesLoc, axisLoc, seriesLoc, this.lastAxesMap, this.lastAxisMap, this.lastSeriesMap);\n"
                +
                "    }\n" +
                "\n" +
                "    private " + outputClassNameShort + " make(final ChartImpl chart){\n" +
                "        final BaseFigureImpl figure = chart.figure();\n" +
                "        final ChartLocation chartLoc = new ChartLocation(chart);\n" +
                "        final AxesLocation axesLoc = resolveLastAxes(figure, chartLoc);\n" +
                "        final AxisLocation axisLoc = resolveLastAxis(figure, axesLoc);\n" +
                "        final SeriesLocation seriesLoc = resolveLastSeries(figure, axesLoc);\n" +
                "        return new " + outputClassNameShort
                + "(figure, chartLoc, axesLoc, axisLoc, seriesLoc, this.lastAxesMap, this.lastAxisMap, this.lastSeriesMap);\n"
                +
                "    }\n" +
                "\n" +
                "    private " + outputClassNameShort + " make(final AxesImpl axes){\n" +
                "        final BaseFigureImpl figure = axes.chart().figure();\n" +
                "        final ChartLocation chartLoc = new ChartLocation(axes.chart());\n" +
                "        final AxesLocation axesLoc = new AxesLocation(axes);\n" +
                "        final AxisLocation axisLoc = resolveLastAxis(figure, axesLoc);\n" +
                "        final SeriesLocation seriesLoc = resolveLastSeries(figure, axesLoc);\n" +
                "        return new " + outputClassNameShort
                + "(figure, chartLoc, axesLoc, axisLoc, seriesLoc, this.lastAxesMap, this.lastAxisMap, this.lastSeriesMap);\n"
                +
                "    }\n" +
                "\n" +
                "    private " + outputClassNameShort + " make(final AxesImpl axes, final AxisImpl axis){\n" +
                "        final BaseFigureImpl figure = axis.chart().figure();\n" +
                "        final ChartLocation chartLoc = new ChartLocation(axis.chart());\n" +
                "        final AxesLocation axesLoc = axes == null ? this.lastAxes : new AxesLocation(axes);\n" +
                "        final AxisLocation axisLoc = new AxisLocation(axis);\n" +
                "        final SeriesLocation seriesLoc = resolveLastSeries(figure, axesLoc);\n" +
                "        return new " + outputClassNameShort
                + "(figure, chartLoc, axesLoc, axisLoc, seriesLoc, this.lastAxesMap, this.lastAxisMap, this.lastSeriesMap);\n"
                +
                "    }\n" +
                "\n" +
                "    private " + outputClassNameShort + " make(final SeriesInternal series){\n" +
                "        final BaseFigureImpl figure = series.axes().chart().figure();\n" +
                "        final ChartLocation chartLoc = new ChartLocation(series.axes().chart());\n" +
                "        final AxesLocation axesLoc = new AxesLocation(series.axes());\n" +
                "        final AxisLocation axisLoc = resolveLastAxis(figure, axesLoc);\n" +
                "        final SeriesLocation seriesLoc = new SeriesLocation(series);\n" +
                "        return new " + outputClassNameShort
                + "(figure, chartLoc, axesLoc, axisLoc, seriesLoc, this.lastAxesMap, this.lastAxisMap, this.lastSeriesMap);\n"
                +
                "    }\n" +
                "\n" +
                "\n" +
                "    private BaseFigureImpl figure(final BaseFigureImpl figure) { return figure; }\n" +
                "\n" +
                "    private ChartImpl chart(final BaseFigureImpl figure) { \n" +
                "        if( this.lastChart == null ) { return figure.newChart(); } \n" +
                "        ChartImpl c = this.lastChart.get(figure);\n" +
                "        if( c == null ) { c = figure.newChart(); }\n" +
                "        return c;\n" +
                "    }\n" +
                "\n" +
                "    private AxesImpl axes(final BaseFigureImpl figure) {\n" +
                "        if( this.lastAxes == null ) { return chart(figure).newAxes(); }\n" +
                "        AxesImpl a = this.lastAxes.get(figure);\n" +
                "        if( a == null ) {\n" +
                "            ChartImpl c = chart(figure);\n" +
                "            a = c.newAxes();\n " +
                "        }\n" +
                "        return a;\n" +
                "    }\n" +
                "\n" +
                "    private AxisImpl axis(final BaseFigureImpl figure) {\n" +
                "        if( this.lastAxis == null ) { throw new PlotRuntimeException(\"No axes have been selected.\", figure); }\n"
                +
                "        AxisImpl a = this.lastAxis.get(figure);\n" +
                "        if( a == null ) { throw new PlotRuntimeException(\"No axes have been selected.\", figure); }\n"
                +
                "        return a;\n" +
                "    }\n" +
                "\n" +
                "    private Series series(final BaseFigureImpl figure) {\n" +
                "        if( this.lastSeries == null ) { throw new PlotRuntimeException(\"No series has been selected.\", figure); }\n"
                +
                "        Series s = this.lastSeries.get(figure);\n" +
                "        if( s == null ) { throw new PlotRuntimeException(\"No series has been selected.\", figure); }\n"
                +
                "        return s;\n" +
                "    }\n" +
                "\n" +
                "\n";
    }

    private String createFigureFuncs() {
        return "    /**\n" +
                "     * Creates a displayable figure that can be sent to the client.\n" +
                "     *\n" +
                "     * @return a displayable version of the figure\n" +
                "     */\n" +
                "    " + (isInterface ? "" : "@Override public ") + "Figure" + (isInterface ? "" : "Impl") + " show()" +
                (isInterface ? ";\n"
                        : " {\n"
                                + indent(2) + "final BaseFigureImpl fc = onDisplay();\n"
                                + indent(2) + "return new FigureWidget(make(fc));\n"
                                + indent(1) + "}\n")
                +
                (isInterface ? "\n" +
                        "\n" +
                        "    @Override  Figure save( java.lang.String saveLocation );\n" +
                        "\n" +
                        "    @Override  Figure save( java.lang.String saveLocation, int width, int height );\n" +
                        "\n" +
                        "    @Override  Figure save( java.lang.String saveLocation, boolean wait, long timeoutSeconds );\n"
                        +
                        "\n" +
                        "    @Override  Figure save( java.lang.String saveLocation, int width, int height, boolean wait, long timeoutSeconds );\n"
                        : "\n" + "    @Override public  FigureImpl save( java.lang.String saveLocation ) {\n" +
                                "        final BaseFigureImpl fc = onDisplay();\n" +
                                "        figure(fc).save( saveLocation );\n" +
                                "        return make(fc);\n" +
                                "    }\n" +
                                "\n" +
                                "    @Override public  FigureImpl save( java.lang.String saveLocation, int width, int height ) {\n"
                                +
                                "        final BaseFigureImpl fc = onDisplay();\n" +
                                "        figure(fc).save( saveLocation, width, height );\n" +
                                "        return make(fc);\n" +
                                "    }\n" +
                                "\n" +
                                "\n" +
                                "    @Override public  FigureImpl save( java.lang.String saveLocation, boolean wait, long timeoutSeconds ) {\n"
                                +
                                "        final BaseFigureImpl fc = onDisplay();\n" +
                                "        figure(fc).save( saveLocation, wait, timeoutSeconds );\n" +
                                "        return make(fc);\n" +
                                "    }\n" +
                                "\n" +
                                "    @Override public  FigureImpl save( java.lang.String saveLocation, int width, int height, boolean wait, long timeoutSeconds ) {\n"
                                +
                                "        final BaseFigureImpl fc = onDisplay();\n" +
                                "        figure(fc).save( saveLocation, width, height, wait, timeoutSeconds );\n" +
                                "        return make(fc);\n" +
                                "    }\n\n")
                + (isInterface ? ""
                        : "    /**\n" +
                                "     * Perform operations required to display the plot.\n" +
                                "     */\n" +
                                "    private BaseFigureImpl onDisplay() {\n" +
                                "        final FigureImpl fig = applyFunctionalProperties();\n" +
                                "        final BaseFigureImpl fc = fig.figure.copy();\n" +
                                "        fc.validateInitialization();\n" +
                                "        return fc;\n" +
                                "    }\n\n" +
                                "    /**\n" +
                                "     * Apply functions to our tables and consolidate them.\n" +
                                "     */\n" +
                                "    private FigureImpl applyFunctionalProperties() {\n" +
                                "        final Map<Table, java.util.Set<java.util.function.Function<Table, Table>>> tableFunctionMap = getFigure().getTableFunctionMap();\n"
                                +
                                "        final Map<io.deephaven.engine.table.TableMap, java.util.Set<java.util.function.Function<io.deephaven.engine.table.TableMap, io.deephaven.engine.table.TableMap>>> tableMapFunctionMap = getFigure().getTableMapFunctionMap();\n"
                                +
                                "        final java.util.List<io.deephaven.plot.util.functions.FigureImplFunction> figureFunctionList = getFigure().getFigureFunctionList();\n"
                                +
                                "        final Map<Table, Table> finalTableComputation = new HashMap<>();\n" +
                                "        final Map<io.deephaven.engine.table.TableMap, io.deephaven.engine.table.TableMap> finalTableMapComputation = new HashMap<>();\n"
                                +
                                "        final java.util.Set<Table> allTables = new java.util.HashSet<>();\n" +
                                "        final java.util.Set<io.deephaven.engine.table.TableMap> allTableMaps = new java.util.HashSet<>();\n"
                                +
                                "\n" +
                                "        for(final io.deephaven.plot.util.tables.TableHandle h : getFigure().getTableHandles()) {\n"
                                +
                                "            allTables.add(h.getTable());\n" +
                                "        }\n" +
                                "\n" +
                                "        for(final io.deephaven.plot.util.tables.TableMapHandle h : getFigure().getTableMapHandles()) {\n"
                                +
                                "            if(h instanceof io.deephaven.plot.util.tables.TableBackedTableMapHandle) {\n"
                                +
                                "                allTables.add(((io.deephaven.plot.util.tables.TableBackedTableMapHandle) h).getTable());\n"
                                +
                                "            }\n" +
                                "            if(h.getTableMap() != null) {\n" +
                                "                allTableMaps.add(h.getTableMap());\n" +
                                "            }\n" +
                                "        }\n" +
                                "\n" +
                                "        for(final Table initTable : allTables) {\n" +
                                "            if(tableFunctionMap.get(initTable) != null) {\n" +
                                "\n" +
                                "                finalTableComputation.computeIfAbsent(initTable, t -> {\n" +
                                "                    final java.util.Set<java.util.function.Function<Table, Table>> functions = tableFunctionMap.get(initTable);\n"
                                +
                                "                    Table resultTable = initTable;\n" +
                                "\n" +
                                "                    for(final java.util.function.Function<Table, Table> f : functions) {\n"
                                +
                                "                        resultTable = f.apply(resultTable);\n" +
                                "                    }\n" +
                                "\n" +
                                "                    return resultTable;\n" +
                                "                });\n" +
                                "            } else {\n" +
                                "                finalTableComputation.put(initTable, initTable);\n" +
                                "            }\n" +
                                "        }\n" +
                                "\n" +
                                "\n" +
                                "        for(final io.deephaven.plot.util.tables.TableHandle h : getFigure().getTableHandles()) {\n"
                                +
                                "            h.setTable(finalTableComputation.get(h.getTable()));\n" +
                                "        }\n" +
                                "\n" +
                                "        for(final io.deephaven.plot.util.tables.TableMapHandle h : getFigure().getTableMapHandles()) {\n"
                                +
                                "            if(h instanceof io.deephaven.plot.util.tables.TableBackedTableMapHandle) {\n"
                                +
                                "                ((io.deephaven.plot.util.tables.TableBackedTableMapHandle) h).setTable(finalTableComputation.get(((io.deephaven.plot.util.tables.TableBackedTableMapHandle) h).getTable()));\n"
                                +
                                "            }\n" +
                                "        }\n" +
                                "\n" +
                                "        for(final io.deephaven.engine.table.TableMap initTableMap : allTableMaps) {\n"
                                +
                                "            if(tableMapFunctionMap.get(initTableMap) != null) {\n" +
                                "                finalTableMapComputation.computeIfAbsent(initTableMap, t -> {\n" +
                                "                    final java.util.Set<java.util.function.Function<io.deephaven.engine.table.TableMap, io.deephaven.engine.table.TableMap>> functions = tableMapFunctionMap.get(initTableMap);\n"
                                +
                                "                    io.deephaven.engine.table.TableMap resultTableMap = initTableMap;\n"
                                +
                                "\n" +
                                "                    for(final java.util.function.Function<io.deephaven.engine.table.TableMap, io.deephaven.engine.table.TableMap> f : functions) {\n"
                                +
                                "                        resultTableMap = f.apply(resultTableMap);\n" +
                                "                    }\n" +
                                "\n" +
                                "                    return resultTableMap;\n" +
                                "                });\n" +
                                "            } else {\n" +
                                "                finalTableMapComputation.put(initTableMap, initTableMap);\n" +
                                "            }\n" +
                                "        }\n" +
                                "\n" +
                                "        for(final io.deephaven.plot.util.tables.TableMapHandle h : getFigure().getTableMapHandles()) {\n"
                                +
                                "            h.setTableMap(finalTableMapComputation.get(h.getTableMap()));\n" +
                                "        }\n" +
                                "\n" +
                                "        FigureImpl finalFigure = this;\n" +
                                "        for(final java.util.function.Function<FigureImpl, FigureImpl> figureFunction : figureFunctionList) {\n"
                                +
                                "            finalFigure = figureFunction.apply(finalFigure);\n" +
                                "        }\n" +
                                "\n" +
                                "        tableFunctionMap.clear();\n" +
                                "        tableMapFunctionMap.clear();\n" +
                                "        figureFunctionList.clear();\n" +
                                "\n" +
                                "        return finalFigure;\n" +
                                "    }"
                                + "\n"
                                + "\n");
    }

    private static String createInstanceGetter(final JavaFunction f) {
        switch (f.getClassName()) {
            case "io.deephaven.plot.BaseFigure":
                return "figure(fc)";
            case "io.deephaven.plot.Chart":
                return "chart(fc)";
            case "io.deephaven.plot.Axes":
                return "axes(fc)";
            case "io.deephaven.plot.Axis":
                return "axis(fc)";
            case "io.deephaven.plot.datasets.DataSeries":
                return "series(fc)";
            case "io.deephaven.plot.datasets.multiseries.MultiSeries":
                return "series(fc)";
            default:
                System.out.println("Don't know how to create instance getter: className=" + f.getClassName());
                return "WTF(fc)";
        }
    }

    private String createFunctionSignature(final JavaFunction f) {
        String s = "    " + (isInterface ? "@Override " : "@Override public ");

        if (f.getTypeParameters().length > 0) {
            s += "<";

            for (int i = 0; i < f.getTypeParameters().length; i++) {
                if (i != 0) {
                    s += ",";
                }

                TypeVariable<Method> t = f.getTypeParameters()[i];
                log.info("BOUNDS: " + Arrays.toString(t.getBounds()));
                s += t;

                Type[] bounds = t.getBounds();

                if (bounds.length != 1) {
                    throw new RuntimeException("Unsupported bounds: " + Arrays.toString(bounds));
                }

                Type bound = bounds[0];

                if (!bound.equals(Object.class)) {
                    s += " extends " + bound.getTypeName();
                }

            }

            s += ">";
        }

        s += " " + outputClassNameShort + " " + f.getMethodName() + "(";

        for (int i = 0; i < f.getParameterTypes().length; i++) {
            if (i != 0) {
                s += ",";
            }

            Type t = f.getParameterTypes()[i];

            String typeString = t.getTypeName().replace("$", ".");

            if (f.isVarArgs() && i == f.getParameterTypes().length - 1) {
                final int index = typeString.lastIndexOf("[]");
                typeString = typeString.substring(0, index) + "..." + typeString.substring(index + 2);
            }

            s += " " + typeString + " " + f.getParameterNames()[i];
        }

        s += " )";

        return s;
    }

    private static String createCallArgs(final JavaFunction f) {
        String callArgs = "";

        for (int i = 0; i < f.getParameterTypes().length; i++) {
            if (i != 0) {
                callArgs += ",";
            }

            callArgs += " " + f.getParameterNames()[i];
        }

        return callArgs;
    }

    private String createFunction(final JavaFunction f) {
        final String returnType = f.getReturnType().getTypeName().replace("$", ".");
        final Class returnClass = f.getReturnClass();
        final JavaFunction signature = signature(f);

        String s = createFunctionSignature(f);

        if (isInterface) {
            return s + ";\n";
        }

        final String callArgs = createCallArgs(f);

        s += " {\n" + indent(2) + "final BaseFigureImpl fc = this.figure.copy();\n";

        if (returnClass != null && BaseFigure.class.isAssignableFrom(returnClass)) {
            s += indent(2) + createInstanceGetter(f) + "." + signature.getMethodName() + "(" + callArgs + ");\n" +
                    indent(2) + "return make(fc);\n";
        } else if (returnClass != null && Chart.class.isAssignableFrom(returnClass)) {
            s += indent(2) + "final ChartImpl chart = (ChartImpl) " + createInstanceGetter(f) + "."
                    + signature.getMethodName() + "(" + callArgs + ");\n" +
                    indent(2) + "return make(chart);\n";
        } else if (returnClass != null && Axes.class.isAssignableFrom(returnClass)) {
            s += indent(2) + "final AxesImpl axes = (AxesImpl) " + createInstanceGetter(f) + "."
                    + signature.getMethodName() + "(" + callArgs + ");\n" +
                    indent(2) + "return make(axes);\n";
        } else if (returnClass != null && Axis.class.isAssignableFrom(returnClass)
                && f.getClassName().equals("io.deephaven.plot.Axes")) {
            s += indent(2) + "final AxesImpl axes = " + createInstanceGetter(f) + ";\n";
            s += indent(2) + "final AxisImpl axis = (AxisImpl) axes." + signature.getMethodName() + "(" + callArgs
                    + ");\n" +
                    indent(2) + "return make(axes, axis);\n";
        } else if (returnClass != null && Axis.class.isAssignableFrom(returnClass)) {
            s += indent(2) + "final AxisImpl axis = (AxisImpl) " + createInstanceGetter(f) + "."
                    + signature.getMethodName() + "(" + callArgs + ");\n" +
                    indent(2) + "return make(null, axis);\n";
        } else if (returnClass != null && DataSeries.class.isAssignableFrom(returnClass)) {
            s += indent(2) + "final DataSeriesInternal series = (DataSeriesInternal) " + createInstanceGetter(f) + "."
                    + signature.getMethodName() + "(" + callArgs + ");\n" +
                    indent(2) + "return make(series);\n";
        } else if (returnClass != null && Series.class.isAssignableFrom(returnClass)) {
            s += indent(2) + "final SeriesInternal series = (SeriesInternal) " + createInstanceGetter(f) + "."
                    + signature.getMethodName() + "(" + callArgs + ");\n" +
                    indent(2) + "return make(series);\n";
        } else if (returnClass != null && MultiSeries.class.isAssignableFrom(returnClass)) {
            s += indent(2) + "final " + returnClass.getSimpleName() + " mseries = " + createInstanceGetter(f) + "."
                    + signature.getMethodName() + "(" + callArgs + ");\n" +
                    indent(2) + "return make((SeriesInternal) mseries);\n";
        } else if (returnClass != null && void.class.equals(returnClass)) {
            s += indent(2) + createInstanceGetter(f) + "." + signature.getMethodName() + "(" + callArgs + ");\n" +
                    indent(2) + "return make(fc);\n";
        } else {
            System.out.println("WARN: UnsupportedReturnType: " + returnType + " " + f);

            s += indent(2) + createInstanceGetter(f) + "." + signature.getMethodName() + "(" + callArgs + ");\n" +
                    indent(2) + "return make(fc);\n";
        }

        s += indent(1) + "}\n";

        return s;
    }

    private String createSignatureGroupFunction(final TreeSet<JavaFunction> fs) {

        if (fs.isEmpty()) {
            throw new IllegalStateException("Signature group contains no functions!");
        }

        final JavaFunction f0 = fs.first();
        final JavaFunction s0 = signature(f0);


        final String signature = createFunctionSignature(s0);

        if (isInterface) {
            return signature + ";\n";
        }

        final String callArgs = createCallArgs(f0);

        String s = signature;
        s += " {\n" +
                indent(2) + "final BaseFigureImpl fc = this.figure.copy();\n" +
                indent(2) + "Series series = series(fc);\n";

        boolean firstFunc = true;

        for (final JavaFunction f : fs) {
            final String returnType = f.getReturnType().getTypeName().replace("$", ".");
            Class returnClass = f.getReturnClass();

            if (returnClass == null) {
                throw new UnsupportedOperationException("Null return class. f=" + f);
            }

            if (firstFunc) {
                s += indent(2) + "if( series instanceof " + f.getClassNameShort() + "){\n";
            } else {
                s += indent(2) + "} else if( series instanceof " + f.getClassNameShort() + "){\n";
            }

            s += indent(3) + returnClass.getSimpleName() + " result = ((" + f.getClassNameShort() + ") series)."
                    + f.getMethodName() + "(" + callArgs + ");\n";

            if (DataSeries.class.isAssignableFrom(returnClass)) {
                s += indent(3) + "return make((DataSeriesInternal)result);\n";
            } else if (MultiSeries.class.isAssignableFrom(returnClass) || Series.class.isAssignableFrom(returnClass)) {
                s += indent(3) + "return make((SeriesInternal)result);\n";
            } else {
                throw new IllegalStateException("UnsupportedReturnType: " + returnType + " " + f);
                // System.out.println("WARN: UnsupportedReturnType: " + returnType + " " + f);
                // s += indent(3) + "return make(fc);";
            }

            s += indent(2) + "} ";

            if (!f.getClassNameShort().equals("MultiSeries") && !f.getClassNameShort().equals("XYDataSeriesFunction")) {
                s += makeMultiSeriesGetter(f);
            }

            firstFunc = false;
        }
        s += "else {\n" +
                indent(3)
                + "throw new PlotUnsupportedOperationException(\"Series type does not support this method.  seriesType=\" + series.getClass() + \" method='"
                + signature.trim() + "'\", figure);\n" +
                indent(2) + "}\n";
        s += indent(1) + "}\n";

        return s;
    }

    private Map<String, TreeSet<GroovyStaticImportGenerator.JavaFunction>> commonSignatureGroups(
            final String[] interfaces) throws ClassNotFoundException {
        final Map<String, TreeSet<GroovyStaticImportGenerator.JavaFunction>> methods = new TreeMap<>();

        final Set<GroovyStaticImportGenerator.JavaFunction> functionSet = new HashSet<>();
        for (String iface : interfaces) {
            final Class<?> c = Class.forName(iface);
            log.info("Processing class: " + c);

            for (final java.lang.reflect.Method m : c.getMethods()) {
                log.info("Processing method (" + c + "): " + m);
                boolean isStatic = Modifier.isStatic(m.getModifiers());
                boolean isPublic = Modifier.isPublic(m.getModifiers());
                boolean isObject = m.getDeclaringClass().equals(Object.class);

                if (!isStatic && isPublic && !isObject) {
                    final GroovyStaticImportGenerator.JavaFunction f = new GroovyStaticImportGenerator.JavaFunction(m);
                    if (functionSet.add(f)) { // avoids repeating methods that have the same parameter types but
                                              // different parameter names
                        final String key = createFunctionSignature(f);
                        final TreeSet<GroovyStaticImportGenerator.JavaFunction> mm =
                                methods.computeIfAbsent(key, k -> new TreeSet<>());
                        mm.add(f);
                    }
                }
            }
        }

        return methods;
    }

    private static String makeMultiSeriesGetter(final JavaFunction f) {
        final String args = createMultiSeriesArgs(f);
        return "else if(series instanceof MultiSeries) {\n" +
                "                final MultiSeries result = ((MultiSeries) series)." + f.getMethodName() + "(" + args
                + ");\n" +
                "                return make((SeriesInternal) result);\n" +
                "        } ";
    }

    private static String createMultiSeriesArgs(JavaFunction f) {
        final Type[] types = f.getParameterTypes();
        final String[] names = f.getParameterNames();
        String args = String.join(", ", names);
        if (!names[names.length - 1].equals("keys")) {
            args += ", io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY";
        }

        return args;
    }

    private static void generateFile(final String devroot, final boolean assertNoChange, final boolean isInterface)
            throws ClassNotFoundException, IOException {

        log.setLevel(Level.WARNING);
        log.warning("Running GenerateFigureImmutable assertNoChange=" + assertNoChange);

        final String[] imports = {
                "io.deephaven.plot.datasets.DataSeriesInternal",
                "io.deephaven.plot.datasets.multiseries.MultiSeriesInternal",
                "io.deephaven.base.verify.Require",
                "java.util.Map",
                "java.util.HashMap",
                "java.util.Arrays",
                "io.deephaven.plot.util.PlotUtils",
                "io.deephaven.plot.errors.PlotRuntimeException",
                "io.deephaven.plot.errors.PlotUnsupportedOperationException"
        };

        final String[] interfaces = {
                "io.deephaven.plot.BaseFigure",
                "io.deephaven.plot.Chart",
                "io.deephaven.plot.Axes",
                "io.deephaven.plot.Axis"
        };

        final String[] seriesInterfaces = {
                "io.deephaven.plot.datasets.DataSeries",
                "io.deephaven.plot.datasets.category.CategoryDataSeries",
                "io.deephaven.plot.datasets.interval.IntervalXYDataSeries",
                "io.deephaven.plot.datasets.ohlc.OHLCDataSeries",
                "io.deephaven.plot.datasets.xy.XYDataSeries",
                "io.deephaven.plot.datasets.multiseries.MultiSeries",
                "io.deephaven.plot.datasets.xy.XYDataSeriesFunction",
                "io.deephaven.plot.datasets.xyerrorbar.XYErrorBarDataSeries",
                "io.deephaven.plot.datasets.categoryerrorbar.CategoryErrorBarDataSeries"
        };

        final List<Predicate<JavaFunction>> skips = Arrays.asList(x -> {
            try {
                return x.equals(new JavaFunction(PlotExceptionCause.class.getMethod("getPlotInfo")));
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }, x -> {
            try {
                return x.equals(new JavaFunction(BaseFigureImpl.class.getMethod("save", String.class))) ||
                        x.equals(new JavaFunction(
                                BaseFigureImpl.class.getMethod("save", String.class, int.class, int.class)))
                        ||
                        x.equals(new JavaFunction(
                                BaseFigureImpl.class.getMethod("save", String.class, boolean.class, long.class)))
                        ||
                        x.equals(new JavaFunction(BaseFigureImpl.class.getMethod("save", String.class, int.class,
                                int.class, boolean.class, long.class)));
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        });


        @SuppressWarnings("unchecked")
        GenerateFigureImmutable gen = new GenerateFigureImmutable(isInterface, imports, interfaces,
                seriesInterfaces, skips, JavaFunction::getMethodName);

        final String code = gen.generateCode();
        log.info("\n\n**************************************\n\n");
        log.info(code);

        String file = devroot + "/Plot/src/main/java/" + gen.outputClass.replace(".", "/") + ".java";

        if (assertNoChange) {
            String oldCode = new String(Files.readAllBytes(Paths.get(file)));
            if (!code.equals(oldCode)) {
                throw new RuntimeException(
                        "Change in generated code.  Run GenerateFigureImmutable or \"./gradlew :Generators:generateFigureImmutable\" to regenerate\n");
            }
        } else {

            PrintWriter out = new PrintWriter(file);
            out.print(code);
            out.close();

            log.warning(gen.outputClass + " written to: " + file);
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {
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

        generateFile(devroot, assertNoChange, false);
        generateFile(devroot, assertNoChange, true);

    }

}
