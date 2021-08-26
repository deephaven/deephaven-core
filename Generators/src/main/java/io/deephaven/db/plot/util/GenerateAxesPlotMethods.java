/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.util;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public class GenerateAxesPlotMethods {

    private static final Logger log = Logger.getLogger(GenerateAxesPlotMethods.class.toString());

    private static final String PLOT_INFO_ID = "new PlotInfo(this, seriesName)";

    private interface Type {

        String getGenericSignature(int index);

        String getVariableType(int index);

        String getIndexableDataCode(String variableName);

        Boolean isTime();

    }

    private static Map<String, Type> getTypes() {

        final Map<String, Type> types = new HashMap<>();

        types.put("short", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return null;
            }

            @Override
            public String getVariableType(int index) {
                return "short[]";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableNumericDataArrayShort(" + variableName + ", " + PLOT_INFO_ID + ")";
            }

            @Override
            public Boolean isTime() {
                return false;
            }
        });

        types.put("int", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return null;
            }

            @Override
            public String getVariableType(int index) {
                return "int[]";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableNumericDataArrayInt(" + variableName + ", " + PLOT_INFO_ID + ")";
            }

            @Override
            public Boolean isTime() {
                return false;
            }
        });

        types.put("long", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return null;
            }

            @Override
            public String getVariableType(int index) {
                return "long[]";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableNumericDataArrayLong(" + variableName + ", " + PLOT_INFO_ID + ")";
            }

            @Override
            public Boolean isTime() {
                return false;
            }
        });

        types.put("float", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return null;
            }

            @Override
            public String getVariableType(int index) {
                return "float[]";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableNumericDataArrayFloat(" + variableName + ", " + PLOT_INFO_ID + ")";
            }

            @Override
            public Boolean isTime() {
                return false;
            }
        });

        types.put("double", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return null;
            }

            @Override
            public String getVariableType(int index) {
                return "double[]";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableNumericDataArrayDouble(" + variableName + ", " + PLOT_INFO_ID + ")";
            }

            @Override
            public Boolean isTime() {
                return false;
            }
        });

        types.put("Date", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return null;
            }

            @Override
            public String getVariableType(int index) {
                return "Date[]";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableNumericDataArrayDate(" + variableName + ", " + PLOT_INFO_ID + ")";
            }

            @Override
            public Boolean isTime() {
                return true;
            }
        });

        types.put("DBDateTime", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return null;
            }

            @Override
            public String getVariableType(int index) {
                return "DBDateTime[]";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableNumericDataArrayDBDateTime(" + variableName + ", " + PLOT_INFO_ID + ")";
            }

            @Override
            public Boolean isTime() {
                return true;
            }
        });

        types.put("Number", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return "T" + index + " extends Number";
            }

            @Override
            public String getVariableType(int index) {
                return "T" + index + "[]";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableNumericDataArrayNumber<>(" + variableName + ", " + PLOT_INFO_ID + ")";
            }

            @Override
            public Boolean isTime() {
                return false;
            }
        });

        // only supporting number types because supporting time types would cause generic erasure conflicts
        types.put("List", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return "T" + index + " extends Number";
            }

            @Override
            public String getVariableType(int index) {
                return "List<T" + index + ">";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableNumericDataListNumber<>(" + variableName + ", " + PLOT_INFO_ID + ")";
            }

            @Override
            public Boolean isTime() {
                return false;
            }
        });

        types.put("Comparable", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return "T" + index + " extends Comparable";
            }

            @Override
            public String getVariableType(int index) {
                return "T" + index + "[]";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableDataArray<>(" + variableName + ", " + PLOT_INFO_ID + ")";
            }

            @Override
            public Boolean isTime() {
                // returning false here because all methods using comparable don't use time args
                return false;
            }
        });

        types.put("List<Comparable>", new Type() {
            @Override
            public String getGenericSignature(int index) {
                return "T" + index + " extends Comparable";
            }

            @Override
            public String getVariableType(int index) {
                return "List<T" + index + ">";
            }

            @Override
            public String getIndexableDataCode(String variableName) {
                return "new IndexableDataArray<>(" + variableName + ".toArray(new Comparable[" + variableName
                        + ".size()]), new PlotInfo(this, seriesName))";
            }

            @Override
            public Boolean isTime() {
                return false;
            }
        });

        return types;
    }

    private static String javadocGenerics(final String[] variableNames, final Type[] variableTypes,
            final String[] genericJavadocs) {
        StringBuilder result = new StringBuilder();
        int genericIndex = 0;

        for (int i = 0; i < variableNames.length; i++) {
            final String sig = variableTypes[i].getGenericSignature(i);
            if (sig != null) {
                result.append(genericJavadocs[genericIndex].replace("$GENERIC$", "<" + sig.split(" ")[0] + ">"));
                genericIndex++;
            }
        }

        return result.toString();
    }

    private static String codeGenericSignature(final Type... variableTypes) {
        final String generics = IntStream.range(0, variableTypes.length)
                .mapToObj(i -> variableTypes[i].getGenericSignature(i))
                .reduce(null, (a, b) -> a == null ? b : b == null ? a : a + "," + b);

        return generics == null ? "" : "<" + generics + ">";
    }

    private static String codeArguments(final String[] variableNames, final Type[] variableTypes) {
        Require.eq(variableNames.length, "variableNames.length", variableTypes.length, "variableTypes.length");

        return IntStream.range(0, variableNames.length)
                .mapToObj(i -> "final " + variableTypes[i].getVariableType(i) + " " + variableNames[i])
                .reduce(null, (a, b) -> a == null ? b : a + ", " + b);
    }

    private static String codeIndexable(final String[] variableNames, final Type[] variableTypes) {
        Require.eq(variableNames.length, "variableNames.length", variableTypes.length, "variableTypes.length");

        return IntStream.range(0, variableNames.length)
                .mapToObj(i -> variableTypes[i].getIndexableDataCode(variableNames[i]))
                .reduce(null, (a, b) -> a == null ? b : a + ", " + b);
    }

    private static String codeTimeAxis(final Type[] variableTypes, final int split) {
        final Type[] varTypes;
        if (split < 0) {
            varTypes = variableTypes;
        } else {
            if (variableTypes.length <= split) {
                throw new IllegalArgumentException("Split is larger than the number of variables!");
            }

            varTypes = new Type[] {variableTypes[0], variableTypes[split]};
        }

        return Arrays.stream(varTypes)
                .map(vt -> vt == null ? null : vt.isTime().toString())
                .reduce(null, (a, b) -> a == null ? b : b == null ? a : a + ", " + b);
    }

    private static String codeTimeAxis(final Type[] variableTypes, final int split, final int def) {
        final Type[] varTypes;
        if (split < 0) {
            varTypes = new Type[] {variableTypes[def]};
        } else {
            if (variableTypes.length <= split) {
                throw new IllegalArgumentException("Split is larger than the number of variables!");
            }

            varTypes = new Type[] {variableTypes[split]};
        }

        return Arrays.stream(varTypes)
                .map(vt -> vt == null ? null : vt.isTime().toString())
                .reduce(null, (a, b) -> a == null ? b : b == null ? a : a + ", " + b);
    }

    private static String codeFunction(final boolean isInterface, final String[] variableNames,
            final Type[] variableTypes, final String prototype, final String[] genericJavadocs,
            final String returnTypeInterface, final String returnTypeImpl) {
        return codeFunction(isInterface, variableNames, variableTypes, prototype, genericJavadocs, returnTypeInterface,
                returnTypeImpl, -1);
    }

    private static String codeFunction(final boolean isInterface, final String[] variableNames,
            final Type[] variableTypes, final String prototype, final String[] genericJavadocs,
            final String returnTypeInterface, final String returnTypeImpl, final int split) {
        final String generic = codeGenericSignature(variableTypes);
        final String args = codeArguments(variableNames, variableTypes);
        final String indexable = codeIndexable(variableNames, variableTypes);
        final String timeAxis = codeTimeAxis(variableTypes, split);
        final String yTimeAxis = prototype.contains("$YTIMEAXIS$)") ? codeTimeAxis(variableTypes, split, 1) : "";
        final String zTimeAxis = prototype.contains("$ZTIMEAXIS$)") ? codeTimeAxis(variableTypes, split, 2) : "";
        final String javadoc = javadocGenerics(variableNames, variableTypes, genericJavadocs);

        final String code = prototype
                .replace("$GENERIC$", generic)
                .replace("$ARGS$", args)
                .replace("$INDEXABLE$", indexable)
                .replace("$TIMEAXIS$", timeAxis)
                .replace("$YTIMEAXIS$", yTimeAxis)
                .replace("$ZTIMEAXIS$", zTimeAxis)
                .replace("$JAVADOCS$", javadoc)
                .replace("$RETURNTYPE$", isInterface ? returnTypeInterface : returnTypeImpl);

        final String rst;
        if (isInterface) {
            int i1 = code.indexOf("public ");
            int i2 = code.indexOf("{", i1);
            rst = "    " + code.substring(0, i2).replace("public ", "").trim() + ";\n";
        } else {
            rst = "    " + code.substring(code.indexOf("public ")).replace("public ", "@Override public ");
        }

        return rst;
    }

    private static ArrayList<ArrayList<Type>> constructTypePossibilities(final String[][] variableTypes,
            final int depth, final ArrayList<Type> types) {
        final ArrayList<ArrayList<Type>> result = new ArrayList<>();

        if (depth >= variableTypes.length) {
            result.add(types);
            return result;
        }

        final Map<String, Type> typeMap = getTypes();

        for (String variableType : variableTypes[depth]) {
            final ArrayList<Type> t = new ArrayList<>(types);
            t.add(typeMap.get(variableType));
            result.addAll(constructTypePossibilities(variableTypes, depth + 1, t));
        }

        return result;
    }

    private static String codeFunction(final boolean isInterface, final String[] variableNames,
            final String[][] variableTypes, final String prototype, final String[] genericJavadocs,
            final String returnTypeInterface, final String returnTypeImpl) {
        Require.eq(variableNames.length, "variableNames.length", variableTypes.length, "variableTypes.length");

        final ArrayList<ArrayList<Type>> typePossibilities =
                constructTypePossibilities(variableTypes, 0, new ArrayList<>());

        return typePossibilities.stream()
                .map(tp -> codeFunction(isInterface, variableNames, tp.toArray(new Type[variableNames.length]),
                        prototype, genericJavadocs, returnTypeInterface, returnTypeImpl))
                .reduce("", (a, b) -> a + "\n" + b);
    }

    private static String codeFunctionRestrictedNumericalVariableTypes(final boolean isInterface,
            final String[] variableNames, final int split, final String prototype, final String[] genericJavadocs,
            final String returnTypeInterface, final String returnTypeImpl) {

        final ArrayList<ArrayList<Type>> typePossibilities = constructRestrictedNumericalTypes(variableNames, split);

        return typePossibilities.stream()
                .map(tp -> codeFunction(isInterface, variableNames, tp.toArray(new Type[variableNames.length]),
                        prototype, genericJavadocs, returnTypeInterface, returnTypeImpl, split))
                .reduce("", (a, b) -> a + "\n" + b);
    }

    private static String codeFunctionRestrictedNumericalVariableTypes(final boolean isInterface,
            final String[] variableNames, final String[] variableTypes, final int split, final String prototype,
            final String[] genericJavadocs,
            final String returnTypeInterface, final String returnTypeImpl) {


        final ArrayList<ArrayList<Type>> typePossibilities =
                constructRestrictedNumericalTypes(variableNames, variableTypes, split);

        return typePossibilities.stream()
                .map(tp -> codeFunction(isInterface, variableNames, tp.toArray(new Type[variableNames.length]),
                        prototype, genericJavadocs, returnTypeInterface, returnTypeImpl, split))
                .reduce("", (a, b) -> a + "\n" + b);
    }

    private static ArrayList<ArrayList<Type>> constructRestrictedNumericalTypes(final String[] variableNames,
            final int split) {
        final ArrayList<ArrayList<Type>> result = new ArrayList<>();

        final Map<String, Type> typeMap = getTypes();

        // all variables are same numerical type
        for (final String s : numberTypes) {
            final Type type = typeMap.get(s);
            final ArrayList<Type> types = new ArrayList<>();

            for (int i = 0; i < variableNames.length; i++) {
                types.add(type);
            }
            result.add(types);
        }

        // all variables are same time type
        for (final String s : timeTypes) {
            final Type type = typeMap.get(s);
            final ArrayList<Type> types = new ArrayList<>();

            for (int i = 0; i < variableNames.length; i++) {
                types.add(type);
            }
            result.add(types);
        }

        // all variables on either side of the split are homogenous
        for (final String s : timeTypes) {
            final Type timeType = typeMap.get(s);
            for (final String s2 : numberTypes) {
                final Type numberType = typeMap.get(s2);

                final ArrayList<Type> types = new ArrayList<>();
                final ArrayList<Type> types1 = new ArrayList<>();
                for (int j = 0; j < variableNames.length; j++) {
                    if (j < split) {
                        types.add(timeType);
                        types1.add(numberType);
                    } else {
                        types.add(numberType);
                        types1.add(timeType);
                    }
                }

                result.add(types);
                result.add(types1);
            }

        }



        return result;
    }

    private static ArrayList<ArrayList<Type>> constructRestrictedNumericalTypes(final String[] variableNames,
            final String[] variableTypes, final int split) {
        final ArrayList<ArrayList<Type>> result = new ArrayList<>();

        final Map<String, Type> typeMap = getTypes();

        // all variables are same numerical type
        for (final String variableType : variableTypes) {
            final Type type1 = typeMap.get(variableType);
            for (final String s : numberTypes) {
                final Type numericType = typeMap.get(s);
                final ArrayList<Type> types = new ArrayList<>();

                for (int i = 0; i < split; i++) {
                    types.add(type1);
                }

                for (int i = split; i < variableNames.length; i++) {
                    types.add(numericType);
                }
                result.add(types);
            }
        }

        // all variables are same time type
        for (final String variableType : variableTypes) {
            final Type type1 = typeMap.get(variableType);
            for (final String s : timeTypes) {
                final Type timeType = typeMap.get(s);
                final ArrayList<Type> types = new ArrayList<>();

                for (int i = 0; i < split; i++) {
                    types.add(type1);
                }

                for (int i = split; i < variableNames.length; i++) {
                    types.add(timeType);
                }
                result.add(types);
            }

            return result;
        }

        return result;
    }

    private static final String[] timeTypes = {
            "Date",
            "DBDateTime"
    };

    private static final String[] numberTypes = {
            "short",
            "int",
            "long",
            "float",
            "double",
            "Number",
            "List"
    };


    private static final String[] numberTimeTypes = new String[timeTypes.length + numberTypes.length];

    static {
        System.arraycopy(timeTypes, 0, numberTimeTypes, 0, timeTypes.length);
        System.arraycopy(numberTypes, 0, numberTimeTypes, timeTypes.length, numberTypes.length);
    }

    private static void generate(final boolean assertNoChange, final String file, final boolean isInterface)
            throws IOException {


        final String headerMessage = "CODE BELOW HERE IS GENERATED -- DO NOT EDIT BY HAND";
        final String headerComment = "//////////////////////////////";
        final String headerSpace = "    ";
        final String header = headerSpace + headerComment + " " + headerMessage + " " + headerComment;
        final String header2 =
                headerSpace + headerComment + " TO REGENERATE RUN GenerateAxesPlotMethods " + headerComment;
        final String header3 =
                headerSpace + headerComment + " AND THEN RUN GeneratePlottingConvenience " + headerComment;

        StringBuilder code = new StringBuilder(header + "\n" + header2 + "\n" + header3 + "\n\n\n");

        code.append(
                codeFunction(isInterface, new String[] {"x", "y"}, new String[][] {numberTimeTypes, numberTimeTypes},
                        "    /**\n" +
                                "     * Creates an XY plot.\n" +
                                "     *\n" +
                                "     * @param seriesName name of the created dataset\n" +
                                "     * @param x x-values\n" +
                                "     * @param y y-values\n" +
                                "$JAVADOCS$" +
                                "     * @return dataset created for plot\n" +
                                "     */\n" +
                                "    public $GENERIC$ $RETURNTYPE$ plot(final Comparable seriesName, $ARGS$) {\n" +
                                "        return plot(seriesName, $INDEXABLE$, $TIMEAXIS$);\n" +
                                "    }\n",
                        new String[] {
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n"
                        }, "XYDataSeries", "XYDataSeriesArray"));


        for (String c : numberTypes) {
            final String[] cs = {c};
            code.append(codeFunction(isInterface, new String[] {"time", "open", "high", "low", "close"},
                    new String[][] {timeTypes, cs, cs, cs, cs},
                    "    /**\n" +
                            "     * Creates an open-high-low-close plot.\n" +
                            "     *\n" +
                            "     * @param seriesName name of the created dataset\n" +
                            "     * @param time time data\n" +
                            "     * @param open open data\n" +
                            "     * @param high high data\n" +
                            "     * @param low low data\n" +
                            "     * @param close close data\n" +
                            "$JAVADOCS$" +
                            "     * @return dataset created by the plot\n" +
                            "     */\n" +
                            "    public $GENERIC$ $RETURNTYPE$ ohlcPlot(final Comparable seriesName, $ARGS$) {\n" +
                            "        return ohlcPlot(seriesName, $INDEXABLE$);\n" +
                            "    }\n",
                    new String[] {
                            "     * @param $GENERIC$ open data type\n",
                            "     * @param $GENERIC$ high data type\n",
                            "     * @param $GENERIC$ low data type\n",
                            "     * @param $GENERIC$ close data type\n",
                    }, "OHLCDataSeries", "OHLCDataSeriesArray"));
        }


        code.append(codeFunction(isInterface, new String[] {"x"}, new String[][] {numberTypes},
                "    /**\n" +
                        "     * Creates a histogram.\n" +
                        "     *\n" +
                        "     * @param seriesName name of the created dataset\n" +
                        "     * @param x data\n" +
                        "     * @param nbins number of bins\n" +
                        "$JAVADOCS$" +
                        "     * @return dataset created by the plot\n" +
                        "     */\n" +
                        "    public $GENERIC$ $RETURNTYPE$ histPlot(final Comparable seriesName, $ARGS$, final int nbins) {\n"
                        +
                        "        return histPlot(seriesName, PlotUtils.doubleTable(x, \"Y\"), \"Y\", nbins);\n" +
                        "    }\n",
                new String[] {
                        "     * @param $GENERIC$ data type\n",
                }, "IntervalXYDataSeries", "IntervalXYDataSeriesArray"));


        code.append(codeFunction(isInterface, new String[] {"x"}, new String[][] {numberTypes},
                "    /**\n" +
                        "     * Creates a histogram.\n" +
                        "     *\n" +
                        "     * @param seriesName name of the created dataset\n" +
                        "     * @param x data\n" +
                        "     * @param rangeMin minimum of the range\n" +
                        "     * @param rangeMax maximum of the range\n" +
                        "     * @param nbins number of bins\n" +
                        "$JAVADOCS$" +
                        "     * @return dataset created by the plot\n" +
                        "     */\n" +
                        "    public $GENERIC$ $RETURNTYPE$ histPlot(final Comparable seriesName, $ARGS$, final double rangeMin, final double rangeMax, final int nbins) {\n"
                        +
                        "        return histPlot(seriesName, PlotUtils.doubleTable(x, \"Y\"), \"Y\", rangeMin, rangeMax, nbins);\n"
                        +
                        "    }\n",
                new String[] {
                        "     * @param $GENERIC$ data type\n",
                }, "IntervalXYDataSeries", "IntervalXYDataSeriesArray"));


        code.append(codeFunctionRestrictedNumericalVariableTypes(isInterface,
                new String[] {"x", "xLow", "xHigh", "y", "yLow", "yHigh"}, 3,
                "    /**\n" +
                        "     * Creates an XY plot with error bars in both the x and y directions.\n" +
                        "     *\n" +
                        "     * @param seriesName name of the created dataset\n" +
                        "     * @param x x-values\n" +
                        "     * @param xLow low value in x dimension\n" +
                        "     * @param xHigh high value in x dimension\n" +
                        "     * @param y y-values\n" +
                        "     * @param yLow low value in y dimension\n" +
                        "     * @param yHigh high value in y dimension\n" +
                        "$JAVADOCS$" +
                        "     * @return dataset created by the plot\n" +
                        "     */\n" +
                        "    public $GENERIC$ $RETURNTYPE$ errorBarXY(final Comparable seriesName, $ARGS$) {\n" +
                        "        return errorBarXY(seriesName, $INDEXABLE$, true, true, $TIMEAXIS$);\n" +
                        "    }\n",
                new String[] {
                        "     * @param $GENERIC$ data type\n",
                        "     * @param $GENERIC$ data type\n",
                        "     * @param $GENERIC$ data type\n",
                        "     * @param $GENERIC$ data type\n",
                        "     * @param $GENERIC$ data type\n",
                        "     * @param $GENERIC$ data type\n"
                }, "XYErrorBarDataSeries", "XYErrorBarDataSeriesArray"));


        code.append(
                codeFunctionRestrictedNumericalVariableTypes(isInterface, new String[] {"x", "xLow", "xHigh", "y"}, 3,
                        "    /**\n" +
                                "     * Creates an XY plot with error bars in the x direction.\n" +
                                "     *\n" +
                                "     * @param seriesName name of the created dataset\n" +
                                "     * @param x x-values\n" +
                                "     * @param xLow low value in x dimension\n" +
                                "     * @param xHigh high value in x dimension\n" +
                                "     * @param y y-values\n" +
                                "$JAVADOCS$" +
                                "     * @return dataset created by the plot\n" +
                                "     */\n" +
                                "    public $GENERIC$ $RETURNTYPE$ errorBarX(final Comparable seriesName, $ARGS$) {\n" +
                                "        return errorBarX(seriesName, $INDEXABLE$, true, false, $TIMEAXIS$);\n" +
                                "    }\n",
                        new String[] {
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n"
                        }, "XYErrorBarDataSeries", "XYErrorBarDataSeriesArray"));


        code.append(
                codeFunctionRestrictedNumericalVariableTypes(isInterface, new String[] {"x", "y", "yLow", "yHigh"}, 1,
                        "    /**\n" +
                                "     * Creates an XY plot with error bars in the y direction.\n" +
                                "     *\n" +
                                "     * @param seriesName name of the created dataset\n" +
                                "     * @param x x-values\n" +
                                "     * @param y y-values\n" +
                                "     * @param yLow low value in y dimension\n" +
                                "     * @param yHigh high value in y dimension\n" +
                                "$JAVADOCS$" +
                                "     * @return dataset created by the plot\n" +
                                "     */\n" +
                                "    public $GENERIC$ $RETURNTYPE$ errorBarY(final Comparable seriesName, $ARGS$) {\n" +
                                "        return errorBarY(seriesName, $INDEXABLE$, false, true, $TIMEAXIS$);\n" +
                                "    }\n",
                        new String[] {
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n",
                                "     * @param $GENERIC$ data type\n"
                        }, "XYErrorBarDataSeries", "XYErrorBarDataSeriesArray"));


        code.append(codeFunctionRestrictedNumericalVariableTypes(isInterface,
                new String[] {"categories", "values", "yLow", "yHigh"}, new String[] {"Comparable", "List<Comparable>"},
                1,
                "    /**\n" +
                        "     * Creates a category error bar plot with whiskers in the y direction.\n" +
                        "     *\n" +
                        "     * @param seriesName name of the created dataset\n" +
                        "     * @param categories discrete data\n" +
                        "     * @param values numeric data\n" +
                        "     * @param yLow low value in y dimension\n" +
                        "     * @param yHigh high value in y dimension\n" +
                        "$JAVADOCS$" +
                        "     * @return dataset created by the plot\n" +
                        "     */\n" +
                        "    public $GENERIC$ $RETURNTYPE$ catErrorBar(final Comparable seriesName, $ARGS$) {\n" +
                        "        return catPlot(new CategoryErrorBarDataSeriesMap(this, dataSeries.nextId(), seriesName, $INDEXABLE$), null, null, $YTIMEAXIS$);\n"
                        +
                        "    }\n",
                new String[] {
                        "     * @param $GENERIC$ type of the categorical data\n",
                        "     * @param $GENERIC$ type of the numeric data\n",
                        "     * @param $GENERIC$ type of the numeric data\n",
                        "     * @param $GENERIC$ type of the numeric data\n",
                        "     * @param $GENERIC$ type of the numeric data\n"
                }, "CategoryDataSeries", "CategoryDataSeriesInternal"));

        code.append(codeFunction(isInterface, new String[] {"categories", "values"},
                new String[][] {{"Comparable", "List<Comparable>"}, numberTimeTypes},
                "    /**\n" +
                        "     * Creates a plot with discrete axis.\n" +
                        "     * Discrete data must not have duplicates.\n" +
                        "     *\n" +
                        "     * @param seriesName name of the created dataset\n" +
                        "     * @param categories discrete data\n" +
                        "     * @param values numeric data\n" +
                        "$JAVADOCS$" +
                        "     * @return dataset created for plot\n" +
                        "     */\n" +
                        "    public $GENERIC$ $RETURNTYPE$ catPlot(final Comparable seriesName, $ARGS$) {\n" +
                        "        return catPlot(seriesName, $INDEXABLE$, $YTIMEAXIS$);\n" +
                        "    }\n",
                new String[] {
                        "     * @param $GENERIC$ type of the categorical data\n",
                        "     * @param $GENERIC$ type of the numeric data\n"
                }, "CategoryDataSeries", "CategoryDataSeriesInternal"));


        code.append(codeFunction(isInterface, new String[] {"categories", "values"},
                new String[][] {{"Comparable", "List<Comparable>"}, numberTypes},
                "    /**\n" +
                        "     * Creates a pie plot.\n" +
                        "     * Categorical data must not have duplicates.\n" +
                        "     *\n" +
                        "     * @param seriesName name of the created dataset\n" +
                        "     * @param categories categories\n" +
                        "     * @param values data values\n" +
                        "$JAVADOCS$" +
                        "     * @return dataset created for plot\n" +
                        "     */\n" +
                        "    public $GENERIC$ $RETURNTYPE$ piePlot(final Comparable seriesName, $ARGS$) {\n" +
                        "        return piePlot(seriesName, $INDEXABLE$);\n" +
                        "    }\n",
                new String[] {
                        "     * @param $GENERIC$ type of the categorical data\n",
                        "     * @param $GENERIC$ type of the numeric data\n"
                }, "CategoryDataSeries", "CategoryDataSeriesInternal"));

        // System.out.println(code);

        final String axes = Files.lines(Paths.get(file)).reduce("\n", (a, b) -> a + "\n" + b);

        int cutPoint = axes.lastIndexOf(header);

        if (cutPoint != axes.indexOf(header)) {
            throw new IllegalArgumentException("Input source code contains two autogenerated sections! file=" + file);
        }

        if (cutPoint < 0) {
            cutPoint = axes.lastIndexOf("}");
        } else {
            cutPoint = axes.lastIndexOf("\n", cutPoint);
        }

        String newaxes = axes.substring(0, cutPoint) + "\n" + code + "\n" + "}\n";
        final String newcode = newaxes.trim();

        // System.out.println(newcode);

        if (assertNoChange) {
            String oldCode = new String(Files.readAllBytes(Paths.get(file)));
            if (!newcode.equals(oldCode)) {
                throw new RuntimeException(
                        "Change in generated code.  Run GenerateAxesPlotMethods or \"./gradlew :Generators:generateAxesPlotMethods\" to regenerate\n");
            }
        } else {

            PrintWriter out = new PrintWriter(file);
            out.print(newcode);
            out.close();

            log.warning(file + " written");
        }
    }

    public static void main(String[] args) throws IOException {

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

        log.setLevel(Level.WARNING);
        log.warning("Running GenerateAxesPlotMethods assertNoChange=" + assertNoChange);

        final String fileIface = devroot + "/Plot/src/main/java/io/deephaven/db/plot/Axes.java";
        final String fileImpl = devroot + "/Plot/src/main/java/io/deephaven/db/plot/AxesImpl.java";

        generate(assertNoChange, fileIface, true);
        generate(assertNoChange, fileImpl, false);

    }

}
