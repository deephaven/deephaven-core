/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.python;

import io.deephaven.db.plot.Figure;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.lang.String;
import java.util.stream.Collectors;

/**
 * Generate the contents of the Plot sub-package in the deephaven package
 */
public class PythonPlottingGenerator {

    private static final Logger log = Logger.getLogger(PythonPlottingGenerator.class.toString());
    private static final String FIGURE_PATH = "io.deephaven.db.plot.Figure";
    private static final String PLOTTING_CONVENIENCE_PATH =
        "io.deephaven.db.plot.PlottingConvenience";
    private static final String[] PLOTTING_CONVENIENCE_DOC_PATHS = {
            "io.deephaven.db.plot.Figure",
            "io.deephaven.gui.color.Color",
            "io.deephaven.db.plot.PlotStyle",
            "io.deephaven.db.plot.LineStyle",
            "io.deephaven.db.plot.Font",
            "io.deephaven.db.plot.themes.Themes",
            "io.deephaven.db.plot.filters.Selectables",
            "io.deephaven.db.plot.composite.ScatterPlotMatrix",
            "io.deephaven.db.plot.FigureFactory",
            "io.deephaven.db.plot.axistransformations.AxisTransforms"
    };

    private static String devroot;
    private static boolean assertNoChange;
    private static String plotPreamble;
    private static String plotOutput;
    private static String figureWrapperPreamble;
    private static String figureWrapperOutput;

    private static final List<String> catPlotWrapper =
        Arrays.asList("catPlot", "catErrorBar", "piePlot");

    private static List<String> docRoot;
    private static List<PythonGeneratorParser.DocstringContainer> figureDocContainer;
    private static List<PythonGeneratorParser.DocstringContainer> plotDocContainer;

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("args[0] = devroot, args[1] = assertNoChange");
        }

        devroot = args[0];
        assertNoChange = Boolean.parseBoolean(args[1]);
        plotPreamble =
            devroot + "/Generators/src/main/java/io/deephaven/pythonPreambles/PlotPreamble.txt";
        plotOutput = devroot + "/Integrations/python/deephaven/Plot/__init__.py";
        figureWrapperPreamble = devroot
            + "/Generators/src/main/java/io/deephaven/pythonPreambles/FigureWrapperPreamble.txt";
        figureWrapperOutput = devroot + "/Integrations/python/deephaven/Plot/figure_wrapper.py";

        docRoot = PythonGeneratorParser.getDefaultDocRoot(devroot);

        log.info("PythonPlottingGenerator - using system file encoding: "
            + System.getProperty("file.encoding"));

        figureDocContainer = PythonGeneratorParser.getDocstringContainer(FIGURE_PATH, docRoot, log);
        plotDocContainer = Arrays.stream(PLOTTING_CONVENIENCE_DOC_PATHS)
            .flatMap(
                path -> PythonGeneratorParser.getDocstringContainer(path, docRoot, log).stream())
            .collect(Collectors.toList());

        createFigureWrapper();
        createInitFile();
    }

    private static void createInitFile() throws ClassNotFoundException, IOException {
        final Set<String> skips = new HashSet<>();
        skips.add("figure");

        // create the code container for the Plot subpackage __init__.py file
        final byte[] encoded = Files.readAllBytes(Paths.get(plotPreamble));
        final StringBuilder code = new StringBuilder(new String(encoded, StandardCharsets.UTF_8));

        PythonGeneratorParser.logGenerationMessage(log, PLOTTING_CONVENIENCE_PATH, plotPreamble,
            plotOutput);
        // create the container for each method's code
        final List<String> generatedMethods = new ArrayList<>();
        createFigureMethod(generatedMethods);

        // find all the methods for plotting convenience
        final PythonGeneratorParser.MethodContainer methodContainer =
            PythonGeneratorParser.getPublicMethodsDetails(PLOTTING_CONVENIENCE_PATH);
        for (final PythonGeneratorParser.MethodSignatureCollection method : methodContainer
            .getMethodSignatures().values()) {
            final String methodName = method.getMethodName();
            // get digested method signature
            final PythonGeneratorParser.MethodSignature methodDigest = method.reduceSignature();
            if (!skips.contains(methodName)) {
                createMethod(methodName, methodDigest, generatedMethods);
            }
        }

        // add in all of the generated methods
        code.append(String.join("\n\n", generatedMethods));
        PythonGeneratorParser.finalizeGeneration(code, assertNoChange, plotOutput,
            PLOTTING_CONVENIENCE_PATH,
            ":Generators:generatePythonFigureWrapper");
    }

    private static void createFigureWrapper() throws ClassNotFoundException, IOException {
        final Set<String> skips = new HashSet<>();
        skips.add("show");

        // create the code container for the Plot/figure_wrapper.py file
        final byte[] encoded = Files.readAllBytes(Paths.get(figureWrapperPreamble));
        final StringBuilder code = new StringBuilder(new String(encoded, StandardCharsets.UTF_8));

        PythonGeneratorParser.logGenerationMessage(log, FIGURE_PATH, figureWrapperPreamble,
            figureWrapperOutput);
        // create the container for each method's code
        final List<String> generatedMethods = new ArrayList<>();
        // find all the methods for Figure

        final PythonGeneratorParser.MethodContainer methodContainer =
            PythonGeneratorParser.getPublicMethodsDetails(FIGURE_PATH);
        for (final PythonGeneratorParser.MethodSignatureCollection method : methodContainer
            .getMethodSignatures().values()) {
            final String methodName = method.getMethodName();
            // get digested method signature
            final PythonGeneratorParser.MethodSignature methodDigest = method.reduceSignature();
            if (!skips.contains(methodName)) {
                createFigureMethod(methodName, methodDigest, generatedMethods);
            }
        }

        // add in all of the generated methods
        code.append(String.join("\n", generatedMethods));
        PythonGeneratorParser.finalizeGeneration(code, assertNoChange, figureWrapperOutput,
            FIGURE_PATH,
            ":Generators:generatePythonFigureWrapper");
    }

    private static void createFigureMethod(final List<String> generatedMethods) {
        String decorator = "";
        final String beginMethod = "def figure(*args):" +
            PythonGeneratorParser.getMethodDocstring(plotDocContainer, "figure", 4) + "\n";
        final String endMethod = "    return FigureWrapper(*args)";
        generatedMethods.add(decorator + beginMethod + endMethod);
    }

    private static void createMethod(final String methodName,
        final PythonGeneratorParser.MethodSignature methodSig,
        final List<String> generatedMethods) {
        final String paramString = methodSig.createPythonParams();
        final Class rClass = methodSig.getReturnClass();
        String decorator = "";
        final String beginMethod = "def " + methodName + "(" + paramString + "):" +
            PythonGeneratorParser.getMethodDocstring(plotDocContainer, methodName, 4) + "\n";
        final String endMethod;

        if (Figure.class.equals(rClass)) {
            endMethod = "    return FigureWrapper()." + methodName + "(" + paramString + ")\n";
        } else {
            decorator = "@_convertArguments\n";
            if ((rClass != null) && (rClass.isArray())) {
                endMethod = "    return list(_plotting_convenience_." + methodName + "("
                    + paramString + "))\n";
            } else {
                endMethod =
                    "    return _plotting_convenience_." + methodName + "(" + paramString + ")\n";
            }
        }
        generatedMethods.add(decorator + beginMethod + endMethod);
    }

    private static void createFigureMethod(final String methodName,
        final PythonGeneratorParser.MethodSignature methodSig,
        final List<String> generatedMethods) {
        final String paramString = methodSig.createPythonParams();
        final String decorator;
        final String endMethod;

        if (catPlotWrapper.contains(methodName)) {
            decorator = "    @_convertCatPlotArguments\n";
        } else {
            decorator = "    @_convertArguments\n";
        }
        endMethod = "    def " + methodName + "(" + methodSig.createPythonParams(true) + "):" +
            PythonGeneratorParser.getMethodDocstring(figureDocContainer, methodName, 8) +
            "\n        return FigureWrapper(figure=self.figure." + methodName + "(" + paramString
            + "))\n";
        generatedMethods.add(decorator + endMethod);
    }
}
