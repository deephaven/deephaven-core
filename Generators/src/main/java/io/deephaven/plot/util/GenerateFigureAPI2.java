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

//todo rename
//todo doc
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
     * A Python method argument.
     */
    private static class PyArg implements Comparable<PyArg>{
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
            this.javaConverter = javaConverter == null ? "_convert_j" : javaConverter
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

        @Override
        public int compareTo(@NotNull PyArg o) {
            final int c1 = Integer.compare(this.precedence, o.precedence);

            if (c1 != 0) {
                return c1;
            }

            return this.name.compareTo(o.name);
        }
    }

    /**
     * A Python function.
     */
    private static class PyFunc {
        private final String name;
        private final String pydoc;
        private final String[] javaFuncs;
        private final String[] requiredParams;

        public PyFunc(final String name, final String[] javaFuncs, final String[] requiredParams, final String pydoc) {
            this.name = name;
            this.pydoc = pydoc;
            this.javaFuncs = javaFuncs;
            this.requiredParams = requiredParams == null ? new String[]{} : requiredParams;
        }

        @Override
        public String toString() {
            return "PyFunc{" +
                    "name='" + name + '\'' +
                    ", pydoc='" + pydoc + '\'' +
                    ", javaFuncs=" + Arrays.toString(javaFuncs) +
                    ", requiredParams=" + Arrays.toString(requiredParams) +
                    '}';
        }

        /**
         * Gets the Java signatures for this method.
         *
         * @param signatures java signatures
         * @return Java signatures for this method.
         */
        public Map<Key, ArrayList<JavaFunction>> getSignatures(final Map<Key, ArrayList<JavaFunction>> signatures){
            final Set<String> funcs = new HashSet<>(Arrays.asList(javaFuncs));
            return new TreeMap<>(
                    signatures
                            .entrySet()
                            .stream()
                            .filter(e->funcs.contains(e.getKey().name))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );
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
    }

    //todo nuke?
//    /**
//     * Convert camel case to snake case.
//     *
//     * @param str input
//     * @return snake case string
//     */
//    private static String camelToSnake(final String str) {
//        String regex = "([a-z])([A-Z]+)";
//        String replacement = "$1_$2";
//        return str.replaceAll(regex, replacement).toLowerCase();
//    }


    /**
     * A map of Java parameter names to Python parameters.
     *
     * @return map of Java parameter names to Python parameters.
     */
    private static Map<String, PyArg> getPyParameters() {
        final Map<String, PyArg> rst = new TreeMap<>();

        final String[] taStr = new String[]{"str"};
        final String[] taStrs = new String[]{"List[str]"};
        final String[] taBool = new String[]{"bool"};
        final String[] taInt = new String[]{"int"};
        final String[] taInts = new String[]{"List[int]"};
        final String[] taFloat = new String[]{"float"};
        final String[] taFloats = new String[]{"List[float]"};
        final String[] taCallable = new String[]{"Callable"};
        final String[] taTable = new String[]{"Table","SelectableDataSet"};
        final String[] taDataCategory = new String[]{"str", "List[str]"};
        final String[] taDataNumeric = new String[]{"str", "List[int]", "List[float]", "List[DateTime]"};
        final String[] taDataTime = new String[]{"str", "List[DateTime]"};

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


        rst.put("seriesName", new PyArg(1, "series_name", taStr, "name of the created dataset", null));
        rst.put("byColumns", new PyArg(9, "by", taStrs, "columns that hold grouping data", null));
        rst.put("t", new PyArg(2, "t", taTable,  "table or selectable data set (e.g. OneClick filterable table)", null));
        rst.put("x", new PyArg(3, "x", taDataNumeric,  "x-values or column name", null));
        rst.put("y", new PyArg(4, "y", taDataNumeric,  "y-values or column name", null));
        rst.put("function", new PyArg(5, "function", taCallable,  "function", null));
        rst.put("hasXTimeAxis", new PyArg(6, "has_x_time_axis", taBool,  "whether to treat the x-values as time data", null)); //todo needed
        rst.put("hasYTimeAxis", new PyArg(7, "has_y_time_axis", taBool,  "whether to treat the y-values as time data", null)); //todo needed?

        rst.put("categories", new PyArg(3, "categories", taDataCategory,  "discrete data or column name", null));
        rst.put("values", new PyArg(4, "values", taDataNumeric,  "numeric data or column name", null));
        rst.put("xLow", new PyArg(5, "x_low", taDataNumeric,  "low value in x dimension", null));
        rst.put("xHigh", new PyArg(6, "x_high", taDataNumeric,  "high value in x dimension", null));
        rst.put("yLow", new PyArg(7, "y_low", taDataNumeric,  "low value in y dimension", null));
        rst.put("yHigh", new PyArg(8, "y_high", taDataNumeric,  "high value in y dimension", null));

        rst.put("id", new PyArg(10, "axes", taInt,  "identifier", null));
        rst.put("name", new PyArg(10, "name", taStr,  "name", null));
        rst.put("names", new PyArg(10, "names", taStrs,  "series names", null));
        rst.put("dim", new PyArg(10, "dim", taInt,  "dimension of the axis", null));
        rst.put("color", new PyArg(10, "color", taColor,  "color", null));
        rst.put("colors", new PyArg(10, "colors", taColors,  "colors", null));
        rst.put("format", new PyArg(10, "format", taAxisFormat,  "axis format", null));
        rst.put("pattern", new PyArg(10, "pattern", taStr,  "axis format pattern", null));
        rst.put("label", new PyArg(10, "label", taStr,  "label", null));
        rst.put("labels", new PyArg(10, "labels", taStrs,  "labels", null));
        rst.put("family", new PyArg(10, "family", taStr,  "font family; if null, set to Arial", null));
        rst.put("font", new PyArg(10, "font", taFont,  "font", null));
        rst.put("size", new PyArg(10, "size", taInt,  "size", null));
        rst.put("style", new PyArg(10, "style", taStr,  "style", null));
        rst.put("calendar", new PyArg(10, "calendar", taBusinessCalendar,  "business calendar", null));
        rst.put("valueColumn", new PyArg(10, "values", taStr,  "column name", null));
        rst.put("rowNum", new PyArg(10, "row", taInt,  "row index in the Figure's grid. The row index starts at 0.", null));
        rst.put("colNum", new PyArg(10, "col", taInt,  "column index in this Figure's grid. The column index starts at 0.", null));
        rst.put("index", new PyArg(10, "index", taInt,  "index from the Figure's grid. The index starts at 0 in the upper left hand corner of the grid and increases going left to right, top to bottom. E.g. for a 2x2 Figure, the indices would be [0, 1] [2, 3].", null));
        rst.put("showColumnNamesInTitle", new PyArg(10, "show_column_names_in_title", taBool,  "whether to show column names in title. If this is true, the title format will include the column name before the comma separated values; otherwise only the comma separated values will be included.", null));
        rst.put("title", new PyArg(10, "title", taStr,  "title", null));
        rst.put("titleColumns", new PyArg(11, "title_columns", taStrs,  "columns to include in the chart title", null));
        rst.put("titleFormat", new PyArg(12, "title_format", taStr,  "a java.text.MessageFormat format string for the chart title", null));
        rst.put("n", new PyArg(10, "width", taInt,  "how many columns wide", null));
        rst.put("npoints", new PyArg(10, "npoints", taInt,  "number of points", null));
        rst.put("xmin", new PyArg(10, "xmin", taFloat,  "range minimum", null));
        rst.put("xmax", new PyArg(11, "xmax", taFloat,  "range minimum", null));
        rst.put("visible", new PyArg(10, "visible", taInt,  "true to draw the design element; false otherwise.", null));
        rst.put("invert", new PyArg(10, "invert", taBool,  "if true, larger values will be closer to the origin; otherwise, smaller values will be closer to the origin.", null));
        rst.put("useLog", new PyArg(10, "use_log", taBool,  "true to use a log axis transform; false to use a linear axis transform.", null));
        rst.put("nbins", new PyArg(15, "nbins", taInt,  "number of bins", null));
        rst.put("maxRowsCount", new PyArg(10, "max_rows", taInt,  "maximum number of row values to show in chart title", null));
        rst.put("min", new PyArg(10, "min", taFloat,  "range minimum", null));
        rst.put("max", new PyArg(11, "max", taFloat,  "range maximum", null));
        rst.put("count", new PyArg(10, "count", taInt,  "number of minor ticks between consecutive major ticks.", null));

        //todo is time the right x-axis label?  Maybe generalize to x?
        rst.put("time", new PyArg(2, "time", taDataTime,  "time x-values.", null));
        rst.put("open", new PyArg(3, "open", taDataNumeric,  "bar open y-values.", null));
        rst.put("high", new PyArg(4, "high", taDataNumeric,  "bar high y-values.", null));
        rst.put("low", new PyArg(5, "low", taDataNumeric,  "bar low y-values.", null));
        rst.put("close", new PyArg(6, "close", taDataNumeric,  "bar close y-values.", null));

        rst.put("orientation", new PyArg(10, "orientation", taStr,  "plot orientation.", null));

        rst.put("path", new PyArg(1, "path", taStr,  "output path.", null));
        rst.put("height", new PyArg(2, "height", taInt,  "figure height.", null));
        rst.put("width", new PyArg(3, "width", taInt,  "figure width.", null));
        rst.put("wait", new PyArg(4, "wait", taBool,  "whether to hold the calling thread until the file is written.", null));
        rst.put("timeoutSeconds", new PyArg(5, "timeout_seconds", taInt,  "timeout in seconds to wait for the file to be written.", null));

        rst.put("rowSpan", new PyArg(10, "row_span", taInt,  "how many rows high.", null));
        rst.put("colSpan", new PyArg(11, "col_span", taInt,  "how many rows wide.", null));
        rst.put("angle", new PyArg(10, "angle", taInt,  "angle in degrees.", null));

        rst.put("gapBetweenTicks", new PyArg(10, "gap", taFloat,  "distance between ticks.", null));
        rst.put("tickLocations", new PyArg(10, "loc", taFloats,  "coordinates of the major tick locations.", null));
        rst.put("transform", new PyArg(10, "transform", taAxisTransform,  "transform.", null));
        rst.put("updateIntervalMillis", new PyArg(10, "millis", taInt,  "milliseconds.", null));

        rst.put("keys", new PyArg(20, "keys", taKey,  "multi-series keys or a column name containing keys.", null));
        rst.put("keyColumn", new PyArg(20, "key_col", taStr,  "colum name specifying category values.", null)); //todo doc/value?

        rst.put("category", new PyArg(1, "category", taStr,  "category.", null)); //todo doc/value?
        rst.put("shape", new PyArg(2, "shape", taShape,  "shape.", null));
        rst.put("shapes", new PyArg(2, "shapes", taShapes,  "shapes.", null));

        rst.put("factor", new PyArg(3, "size", taFactor,  "size.", null));
        rst.put("factors", new PyArg(3, "sizes", taFactors,  "sizes.", null));
        rst.put("group", new PyArg(110, "group", taInt,  "group for the data series.", null));

        //todo ** min and max should be Union[str, float] and should have "values" renamed to "max"/"min"

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

        //todo how to combine these into better composite functions?
        //todo pydocs
        rst.add(new PyFunc("axes", new String[]{"axes"}, null, "TODO pydoc"));
        rst.add(new PyFunc("axes_remove_series", new String[]{"axesRemoveSeries"}, new String[]{"names"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("axis", new String[]{"axis"}, new String[]{"dim"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("axis_color", new String[]{"axisColor"}, null, "TODO pydoc"));
        rst.add(new PyFunc("axis_format", new String[]{"axisFormat", "axisFormatPattern"}, null, "TODO pydoc"));
        rst.add(new PyFunc("axis_label", new String[]{"axisLabel"}, new String[]{"label"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("axis_label_font", new String[]{"axisLabelFont"}, null, "TODO pydoc"));
        rst.add(new PyFunc("business_time", new String[]{"businessTime"}, null, "TODO pydoc"));
        //todo should cat_error_bar be under cat_plot?
        rst.add(new PyFunc("cat_error_bar", new String[]{"catErrorBar", "catErrorBarBy"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("cat_hist_plot", new String[]{"catHistPlot"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("cat_plot", new String[]{"catPlot", "catPlotBy"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("chart", new String[]{"chart"}, null, "TODO pydoc"));
        rst.add(new PyFunc("chart_remove_series", new String[]{"chartRemoveSeries"}, new String[]{"names"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("chart_title", new String[]{"chartTitle"}, null, "TODO pydoc"));
        rst.add(new PyFunc("chart_title_color", new String[]{"chartTitleColor"}, null, "TODO pydoc"));
        rst.add(new PyFunc("chart_title_font", new String[]{"chartTitleFont"}, null, "TODO pydoc"));
        rst.add(new PyFunc("error_bar_color", new String[]{"errorBarColor"}, new String[]{"color"}, "TODO pydoc")); //todo req?
        //todo should error_bar be part of plot?
        rst.add(new PyFunc("error_bar", new String[]{"errorBarX", "errorBarXBy", "errorBarY", "errorBarYBy", "errorBarXY", "errorBarXYBy"}, new String[]{"series_name"}, "TODO pydoc"));

        rst.add(new PyFunc("figure_remove_series", new String[]{"figureRemoveSeries"}, new String[]{"names"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("figure_title", new String[]{"figureTitle"}, new String[]{"title"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("figure_title_color", new String[]{"figureTitleColor"}, null, "TODO pydoc"));
        rst.add(new PyFunc("figure_title_font", new String[]{"figureTitleFont"}, null, "TODO pydoc"));
        rst.add(new PyFunc("func_n_points", new String[]{"funcNPoints"}, new String[]{"npoints"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("func_range", new String[]{"funcRange"}, new String[]{"xmin", "xmax"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("gradient_visible", new String[]{"gradientVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("grid_lines_visible", new String[]{"gridLinesVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("group", new String[]{"group"}, new String[]{"group"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("hist_plot", new String[]{"histPlot"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("invert", new String[]{"invert"}, null, "TODO pydoc"));
        rst.add(new PyFunc("legend_color", new String[]{"legendColor"}, null, "TODO pydoc"));
        rst.add(new PyFunc("legend_font", new String[]{"legendFont"}, null, "TODO pydoc"));
        rst.add(new PyFunc("legend_visible", new String[]{"legendVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("line_color", new String[]{"lineColor"}, new String[]{"color"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("line_style", new String[]{"lineStyle"}, new String[]{"style"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("lines_visible", new String[]{"linesVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("log", new String[]{"log"}, null, "TODO pydoc"));
        rst.add(new PyFunc("max", new String[]{"max"}, null, "TODO pydoc"));
        rst.add(new PyFunc("max_rows_in_title", new String[]{"maxRowsInTitle"}, new String[]{"maxRowsCount"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("min", new String[]{"min"}, null, "TODO pydoc"));
        rst.add(new PyFunc("minor_ticks", new String[]{"minorTicks"}, new String[]{"count"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("minor_ticks_visible", new String[]{"minorTicksVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("new_axes", new String[]{"newAxes"}, null, "TODO pydoc"));
        rst.add(new PyFunc("new_chart", new String[]{"newChart"}, null, "TODO pydoc"));
        rst.add(new PyFunc("ohlc_plot", new String[]{"ohlcPlot", "ohlcPlotBy"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("pie_percent_label_format", new String[]{"piePercentLabelFormat"}, new String[]{"format"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("pie_plot", new String[]{"piePlot"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("plot", new String[]{"plot", "plotBy"}, new String[]{"series_name"}, "TODO pydoc"));
        rst.add(new PyFunc("plot_orientation", new String[]{"plotOrientation"}, new String[]{"orientation"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("plot_style", new String[]{"plotStyle"}, new String[]{"style"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("point_color", new String[]{"pointColor"}, null, "TODO pydoc"));
        rst.add(new PyFunc("point_color_by_y", new String[]{"pointColorByY"}, new String[]{"colors"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("point_color_integer", new String[]{"pointColorInteger"}, new String[]{"colors"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("point_label", new String[]{"pointLabel"}, null, "TODO pydoc"));
        rst.add(new PyFunc("point_label_format", new String[]{"pointLabelFormat"}, new String[]{"format"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("point_shape", new String[]{"pointShape"}, null, "TODO pydoc"));
        rst.add(new PyFunc("point_size", new String[]{"pointSize"}, null, "TODO pydoc"));
        rst.add(new PyFunc("points_visible", new String[]{"pointsVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("range", new String[]{"range"}, new String[]{"min", "max"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("remove_chart", new String[]{"removeChart"}, null, "TODO pydoc"));
        rst.add(new PyFunc("save", new String[]{"save"}, new String[]{"path"}, "TODO pydoc"));
        rst.add(new PyFunc("series", new String[]{"series"}, null, "TODO pydoc"));
        rst.add(new PyFunc("series_color", new String[]{"seriesColor"}, new String[]{"color"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("series_naming_function", new String[]{"seriesNamingFunction"}, new String[]{"function"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("show", new String[]{"show"}, null, "TODO pydoc"));
        rst.add(new PyFunc("span", new String[]{"span", "colSpan", "rowSpan"}, null, "TODO pydoc")); //todo combine with row and col span
        rst.add(new PyFunc("tick_label_angle", new String[]{"tickLabelAngle"}, new String[]{"angle"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("ticks", new String[]{"ticks"}, null, "TODO pydoc"));
        rst.add(new PyFunc("ticks_font", new String[]{"ticksFont"}, null, "TODO pydoc"));
        rst.add(new PyFunc("ticks_visible", new String[]{"ticksVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("tool_tip_pattern", new String[]{"toolTipPattern"}, new String[]{"format"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("transform", new String[]{"transform"}, new String[]{"transform"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("twin", new String[]{"twin"}, null, "TODO pydoc"));
        rst.add(new PyFunc("twin_x", new String[]{"twinX"}, null, "TODO pydoc")); //todo combine with twin?
        rst.add(new PyFunc("twin_y", new String[]{"twinY"}, null, "TODO pydoc")); //todo combine with twin?
        rst.add(new PyFunc("update_interval", new String[]{"updateInterval"}, new String[]{"millis"}, "TODO pydoc"));

        rst.add(new PyFunc("x_axis", new String[]{"xAxis"}, null, "TODO pydoc"));
        rst.add(new PyFunc("x_business_time", new String[]{"xBusinessTime"}, null, "TODO pydoc"));
        rst.add(new PyFunc("x_color", new String[]{"xColor"}, new String[]{"color"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("x_format", new String[]{"xFormat", "xFormatPattern"}, null, "TODO pydoc"));
        rst.add(new PyFunc("x_grid_lines_visible", new String[]{"xGridLinesVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("x_invert", new String[]{"xInvert"}, null, "TODO pydoc"));
        rst.add(new PyFunc("x_label", new String[]{"xLabel"}, new String[]{"label"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("x_label_font", new String[]{"xLabelFont"}, null, "TODO pydoc"));
        rst.add(new PyFunc("x_log", new String[]{"xLog"}, null, "TODO pydoc"));
        rst.add(new PyFunc("x_max", new String[]{"xMax"}, null, "TODO pydoc"));
        rst.add(new PyFunc("x_min", new String[]{"xMin"}, null, "TODO pydoc"));
        rst.add(new PyFunc("x_minor_ticks", new String[]{"xMinorTicks"}, new String[]{"count"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("x_minor_ticks_visible", new String[]{"xMinorTicksVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("x_range", new String[]{"xRange"}, new String[]{"min", "max"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("x_tick_label_angle", new String[]{"xTickLabelAngle"}, new String[]{"angle"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("x_ticks", new String[]{"xTicks"}, null, "TODO pydoc"));
        rst.add(new PyFunc("x_ticks_font", new String[]{"xTicksFont"}, null, "TODO pydoc"));
        rst.add(new PyFunc("x_ticks_visible", new String[]{"xTicksVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("x_tool_tip_pattern", new String[]{"xToolTipPattern"}, new String[]{"format"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("x_transform", new String[]{"xTransform"}, new String[]{"transform"}, "TODO pydoc")); //todo req?

        rst.add(new PyFunc("y_axis", new String[]{"yAxis"}, null, "TODO pydoc"));
        rst.add(new PyFunc("y_business_time", new String[]{"yBusinessTime"}, null, "TODO pydoc"));
        rst.add(new PyFunc("y_color", new String[]{"yColor"}, new String[]{"color"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("y_format", new String[]{"yFormat", "yFormatPattern"}, null, "TODO pydoc"));
        rst.add(new PyFunc("y_grid_lines_visible", new String[]{"yGridLinesVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("y_invert", new String[]{"yInvert"}, null, "TODO pydoc"));
        rst.add(new PyFunc("y_label", new String[]{"yLabel"}, new String[]{"label"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("y_label_font", new String[]{"yLabelFont"}, null, "TODO pydoc"));
        rst.add(new PyFunc("y_log", new String[]{"yLog"}, null, "TODO pydoc"));
        rst.add(new PyFunc("y_max", new String[]{"yMax"}, null, "TODO pydoc"));
        rst.add(new PyFunc("y_min", new String[]{"yMin"}, null, "TODO pydoc"));
        rst.add(new PyFunc("y_minor_ticks", new String[]{"yMinorTicks"}, new String[]{"count"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("y_minor_ticks_visible", new String[]{"yMinorTicksVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("y_range", new String[]{"yRange"}, new String[]{"min", "max"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("y_tick_label_angle", new String[]{"yTickLabelAngle"}, new String[]{"angle"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("y_ticks", new String[]{"yTicks"}, null, "TODO pydoc"));
        rst.add(new PyFunc("y_ticks_font", new String[]{"yTicksFont"}, null, "TODO pydoc"));
        rst.add(new PyFunc("y_ticks_visible", new String[]{"yTicksVisible"}, new String[]{"visible"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("y_tool_tip_pattern", new String[]{"yToolTipPattern"}, new String[]{"format"}, "TODO pydoc")); //todo req?
        rst.add(new PyFunc("y_transform", new String[]{"yTransform"}, new String[]{"transform"}, "TODO pydoc")); //todo req?



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

    private static List<PyArg> pyMethodArgs(final PyFunc pyFunc, final Map<Key, ArrayList<JavaFunction>> sigs) {
        final Map<String, PyArg> pyparams = getPyParameters();
        final Set<PyArg> argSet = new HashSet<>();

        for(final ArrayList<JavaFunction> signatures : sigs.values()) {
            for (final JavaFunction f : signatures) {
                for (final String param : f.getParameterNames()) {
                    final PyArg pyparam = pyparams.get(param);

                    if(pyparam == null) {
                        throw new IllegalArgumentException("Unsupported python parameter: func=" + pyFunc + " param=" + param);
                    }

                    argSet.add(pyparam);
                }
            }
        }

        final List<PyArg> args = new ArrayList<>(argSet);
        args.sort((a,b)-> {
            final boolean ra = pyFunc.isRequired(a);
            final boolean rb = pyFunc.isRequired(b);

            return ra != rb ? (ra ? -1 : 1) : a.compareTo(b);
        });

        return args;
    }

    private static String pyFuncSignature(final PyFunc pyFunc, final List<PyArg> args) {
        final StringBuilder sb = new StringBuilder();

        sb.append(INDENT).append("def ").append(pyFunc.name).append("(\n");
        sb.append(INDENT).append(INDENT).append("self,\n");

        for (final PyArg arg : args) {
            sb.append(INDENT).append(INDENT).append(arg.name).append(": ").append(arg.typeAnnotation());

            if (!pyFunc.isRequired(arg)) {
                sb.append(" = None");
            }

            sb.append(",\n");
        }

        sb.append(INDENT).append(") -> Figure:\n");

        return sb.toString();
    }

    private static String pyDocString(final PyFunc func, final List<PyArg> args){
        final StringBuilder sb = new StringBuilder();

        sb.append(INDENT).append(INDENT).append("\"\"\"").append(func.pydoc).append("\n\n");
        sb.append(INDENT).append(INDENT).append("Args:\n");

        for (final PyArg arg : args) {
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

        final ArrayList<String[]> rst = new ArrayList<>(vals.values());

        rst.sort((first, second) -> {
            final int l1 = first.length;
            final int l2 = second.length;
            final int c1 = Integer.compare(l1, l2);

            if (c1 != 0) {
                return c1;
            }

            for (int i = 0; i < l1; i++) {
                final int c2 = first[i].compareTo(second[i]);
                if (c2 != 0) {
                    return c2;
                }
            }

            return 0;
        });

        return rst;
    }

    private static String pyFuncBody(final PyFunc pyFunc, final List<PyArg> args, final Map<Key, ArrayList<JavaFunction>> signatures) {
        final Map<String, PyArg> pyParameterMap = getPyParameters();

        final StringBuilder sb = new StringBuilder();

        // validate

        for(final PyArg arg:args){
            if(!pyFunc.isRequired(arg)){
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

        for(final PyArg arg:args){
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
                    .append(arg.javaConverter)
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

        final Set<Set<String>> alreadyGenerated = new HashSet<>();

        for (final Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
            final Key key = entry.getKey();
            final ArrayList<JavaFunction> sigs = entry.getValue();
            final List<String[]> argNames = javaArgNames(sigs);

            boolean isFirst = true;

            for (final String[] an : argNames) {
                final Set<String> argSet = new HashSet<>(Arrays.asList(an));

                if(alreadyGenerated.contains(argSet)) {
                    throw new RuntimeException("Java functions have same signature: function=" + pyFunc + " sig=" + Arrays.toString(an));
                } else {
                    alreadyGenerated.add(argSet);
                }

                final String[] quoted_an = Arrays.stream(an).map(s -> "\"" + pyParameterMap.get(s).name + "\"").toArray(String[]::new);

                sb.append(INDENT)
                        .append(INDENT)
                        .append(isFirst ? "if" : "elif")
                        .append(" non_null_params == {")
                        .append(String.join(",", quoted_an))
                        .append("}:\n")
                        .append(INDENT)
                        .append(INDENT)
                        .append(INDENT)
                        .append("return Figure(self.j_figure.")
                        .append(key.name)
                        .append("(")
                        .append(String.join(",", Arrays.stream(an).map(s -> pyParameterMap.get(s).name).toArray(String[]::new)))
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
                .append("raise DHError(f\"unsupported parameter combination: {non_null_params}\")\n");

        return sb.toString();
    }

    private static String generatePythonFunction(final PyFunc func, final Map<Key, ArrayList<JavaFunction>> signatures) {
        final List<PyArg> args = pyMethodArgs(func, signatures);

        final String sig = pyFuncSignature(func, args);
        final String pydocs = pyDocString(func, args);
        final String pybody = pyFuncBody(func, args, signatures);

        return sig +
                pydocs +
                pybody;
    }

    private static String generatePythonClass(final Map<Key, ArrayList<JavaFunction>> signatures) throws IOException {

        final StringBuilder sb = new StringBuilder();

        final String preamble = Files.readString(Path.of(PREAMBLE));
        sb.append(preamble);
        sb.append("\n");

        final List<PyFunc> pyFuncs = getPyFuncs();

        for(final PyFunc pyFunc : pyFuncs) {
            final Map<Key, ArrayList<JavaFunction>> sigs = pyFunc.getSignatures(signatures);

            sigs.forEach((k,v) -> signatures.remove(k));

            final String pyFuncCode = generatePythonFunction(pyFunc, sigs);
            sb.append("\n").append(pyFuncCode);
        }

        return sb.toString();
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {

        final Map<Key, ArrayList<JavaFunction>> signatures = getSignatures();
        final int nSig1 = signatures.size();

        for (int i = 0; i < 10; i++) {
            System.out.println("===========================================================");
        }

        String pyCode = generatePythonClass(signatures);

        for (int i = 0; i < 10; i++) {
            System.out.println("===========================================================");
        }

        System.out.println(pyCode);

        for (int i = 0; i < 10; i++) {
            System.out.println("===========================================================");
        }

        final int nSig2 = signatures.size();
        final int nSigGenerated = nSig1-nSig2;

        System.out.println("GENSTATS: " + nSigGenerated + " of " + nSig1 + "(" + (nSigGenerated/(double)nSig1) + ")");

        for (int i = 0; i < 10; i++) {
            System.out.println("===========================================================");
        }

        for (final Map.Entry<Key, ArrayList<JavaFunction>> entry : signatures.entrySet()) {
            final Key key = entry.getKey();
            final ArrayList<JavaFunction> sigs = entry.getValue();
            printSignature(key, sigs);
        }

    }
}
