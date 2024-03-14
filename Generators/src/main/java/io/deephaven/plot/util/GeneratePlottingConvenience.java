//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.util;

import io.deephaven.gen.GenUtils;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.plot.FigureImpl;
import io.deephaven.gen.JavaFunction;

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

import static io.deephaven.gen.GenUtils.indent;
import static io.deephaven.gen.GenUtils.typesToImport;

/**
 * Create static functions that resolve against the last created instance of a plotting figure class. This is to make a
 * cleaner plotting interface
 */
@SuppressWarnings("StringConcatenationInLoop")
public class GeneratePlottingConvenience {
    // See also GroovyStaticImportGenerator

    private static final Logger log = Logger.getLogger(GeneratePlottingConvenience.class.toString());
    private static final String GRADLE_TASK = ":Generators:generatePlottingConvenience";

    private static final String OUTPUT_CLASS = "io.deephaven.plot.PlottingConvenience";
    private static final String OUTPUT_CLASS_NAME_SHORT = OUTPUT_CLASS.substring(OUTPUT_CLASS.lastIndexOf('.') + 1);


    private final Map<JavaFunction, JavaFunction> nonstaticFunctions = new TreeMap<>();
    private final Map<JavaFunction, JavaFunction> staticFunctions = new TreeMap<>();
    private final Collection<Predicate<JavaFunction>> skips;
    private final Function<JavaFunction, String> functionNamer;

    private GeneratePlottingConvenience(final String[] staticImports, final String[] imports,
            final Collection<Predicate<JavaFunction>> skips, final Function<JavaFunction, String> functionNamer)
            throws ClassNotFoundException {
        this.skips = skips;
        this.functionNamer = functionNamer == null ? JavaFunction::getMethodName : functionNamer;

        for (final String staticImport : staticImports) {
            final int lastDot = staticImport.lastIndexOf(".");
            final String classPath = staticImport.substring(0, lastDot);
            final String methodName = staticImport.substring(lastDot + 1);
            final Class<?> c = Class.forName(classPath, false, Thread.currentThread().getContextClassLoader());
            log.info("Processing static class: " + c);

            final Method[] methods = Arrays.stream(c.getMethods()).filter(
                    m -> m.getName().equals(methodName) && Modifier.isStatic(m.getModifiers())
                            && Modifier.isPublic(m.getModifiers()))
                    .toArray(Method[]::new);

            for (Method m : methods) {
                log.info("Processing static method (" + c + "): " + m);
                addPublicMethod(m, staticFunctions, true);
            }
        }

        for (final String imp : imports) {
            final Class<?> c = Class.forName(imp, false, Thread.currentThread().getContextClassLoader());
            log.info("Processing class: " + c);

            for (final Method m : c.getMethods()) {
                log.info("Processing method (" + c + "): " + m);
                boolean isStatic = Modifier.isStatic(m.getModifiers());
                boolean isPublic = Modifier.isPublic(m.getModifiers());
                boolean isObject = m.getDeclaringClass().equals(Object.class);
                boolean isFigureImmutable = m.getReturnType().equals(FigureImpl.class);
                boolean isFigureImpl = m.getReturnType().equals(BaseFigureImpl.class);

                if (!isStatic && isPublic && !isObject && (isFigureImmutable || isFigureImpl)) {
                    addPublicMethod(m, nonstaticFunctions, false);
                } else {
                    log.info("Not adding method (" + c + "): " + m);
                }
            }
        }
    }


    private boolean skip(final JavaFunction f, final boolean ignore) {
        if (ignore) {
            return false;
        }

        boolean skip = false;
        for (Predicate<JavaFunction> skipCheck : skips) {
            skip = skip || skipCheck.test(f);
        }

        return skip;
    }

    private void addPublicMethod(Method m, Map<JavaFunction, JavaFunction> functions, boolean ignoreSkips) {
        log.info("Processing public method: " + m);

        final JavaFunction f = new JavaFunction(m);
        final JavaFunction signature = f.transform(OUTPUT_CLASS, OUTPUT_CLASS_NAME_SHORT, functionNamer.apply(f), null);

        boolean skip = skip(f, ignoreSkips);

        if (skip) {
            log.info("*** Skipping function: " + f);
            return;
        }

        if (functions.containsKey(signature)) {
            JavaFunction fAlready = functions.get(signature);
            final String message = "Signature Already Present:	" + fAlready + "\t" + signature;
            log.severe(message);
            throw new RuntimeException(message);
        } else {
            log.info("Added public method: " + f);
            functions.put(signature, f);
        }
    }

    private Set<String> generateImports() {
        Set<String> imports = new TreeSet<>();

        final ArrayList<JavaFunction> functions = new ArrayList<>();
        functions.addAll(staticFunctions.values());
        functions.addAll(nonstaticFunctions.values());

        for (JavaFunction f : functions) {
            imports.add(f.getClassName());

            imports.addAll(typesToImport(f.getReturnType()));

            for (Type t : f.getParameterTypes()) {
                imports.addAll(typesToImport(t));
            }
        }

        return imports;
    }

    private String generateCode() {

        String code = GenUtils.javaHeader(GeneratePlottingConvenience.class, GRADLE_TASK);
        code += "package io.deephaven.plot;\n\n";

        Set<String> imports = generateImports();

        for (String imp : imports) {
            code += "import " + imp.replace("$", ".") + ";\n";
        }

        code += "\n";
        code += "/** \n* A library of methods for constructing plots.\n */\n";
        code += "@SuppressWarnings(\"unused\")\n";
        code += "public class " + OUTPUT_CLASS_NAME_SHORT + " {\n";

        for (Map.Entry<JavaFunction, JavaFunction> entries : staticFunctions.entrySet()) {
            final JavaFunction signature = entries.getKey();
            final JavaFunction f = entries.getValue();
            final boolean skip = skip(f, true);

            if (skip) {
                log.info("*** Skipping static function: " + f);
                continue;
            }

            final String s = createFunction(f, signature, true);

            code += s;
            code += "\n";
        }

        for (Map.Entry<JavaFunction, JavaFunction> entries : nonstaticFunctions.entrySet()) {
            final JavaFunction signature = entries.getKey();
            final JavaFunction f = entries.getValue();
            final boolean skip = skip(f, false);

            if (skip) {
                log.info("*** Skipping function: " + f);
                continue;
            }

            final String s = createFunction(f, signature, false);

            code += s;
            code += "\n";
        }

        code += "}\n\n";

        return code;
    }

    private static String createFunction(final JavaFunction f, final JavaFunction signature, final boolean isStatic) {
        final String javadoc = createJavadoc(f, signature);
        final String sigPrefix = "public static";
        final String callArgs = GenUtils.javaArgString(signature, false);
        String funcBody = " {\n";
        funcBody += indent(2) + (f.getReturnType().equals(void.class) ? "" : "return ")
                + (isStatic ? (f.getClassNameShort() + ".") : "FigureFactory.figure().") + f.getMethodName() + "("
                + callArgs + " );\n";
        funcBody += indent(1) + "}\n";
        return GenUtils.javaFunction(signature, sigPrefix, javadoc, funcBody);
    }

    private static String createJavadoc(final JavaFunction f, final JavaFunction signature) {
        return "    /**\n    * See {@link " + f.getClassName() + "#" + signature.getMethodName() + "} \n    **/";
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {

        String devroot = null;
        boolean assertNoChange = false;
        if (args.length == 1) {
            devroot = args[0];
        } else if (args.length == 2) {
            devroot = args[0];
            assertNoChange = Boolean.parseBoolean(args[1]);
        } else {
            System.out.println("Usage: <devroot> [assertNoChange]");
            System.exit(-1);
        }

        log.setLevel(Level.WARNING);
        log.warning("Running GeneratePlottingConvenience assertNoChange=" + assertNoChange);

        final String[] staticImports = {
                "io.deephaven.gui.color.Color.color",
                "io.deephaven.gui.color.Color.colorRGB",
                "io.deephaven.gui.color.Color.colorHSL",
                "io.deephaven.gui.color.Color.colorNames",

                "io.deephaven.plot.PlotStyle.plotStyleNames",

                "io.deephaven.plot.LineStyle.lineEndStyle",
                "io.deephaven.plot.LineStyle.lineEndStyleNames",
                "io.deephaven.plot.LineStyle.lineJoinStyle",
                "io.deephaven.plot.LineStyle.lineJoinStyleNames",
                "io.deephaven.plot.LineStyle.lineStyle",

                "io.deephaven.plot.Font.fontStyle",
                "io.deephaven.plot.Font.fontStyleNames",
                "io.deephaven.plot.Font.font",
                "io.deephaven.plot.Font.fontFamilyNames",
                "io.deephaven.plot.Font.fontStyles",

                "io.deephaven.plot.axistransformations.AxisTransforms.axisTransform",
                "io.deephaven.plot.axistransformations.AxisTransforms.axisTransformNames",

                "io.deephaven.plot.filters.Selectables.oneClick",
                "io.deephaven.plot.filters.Selectables.linked",

                "io.deephaven.plot.composite.ScatterPlotMatrix.scatterPlotMatrix",

                "io.deephaven.plot.FigureFactory.figure",
        };

        final String[] imports = {
                "io.deephaven.plot.FigureImpl",
        };

        final Set<String> keepers = new HashSet<>(Arrays.asList(
                "newAxes",
                "newChart",
                "plot",
                "plotBy",
                "ohlcPlot",
                "ohlcPlotBy",
                "histPlot",
                "catHistPlot",
                "catPlot",
                "catPlotBy",
                "piePlot",
                "errorBarXY",
                "errorBarXYBy",
                "errorBarX",
                "errorBarXBy",
                "errorBarY",
                "errorBarYBy",
                "catErrorBar",
                "catErrorBarBy",
                "treemapPlot"));

        GeneratePlottingConvenience gen = new GeneratePlottingConvenience(staticImports, imports,
                List.of(javaFunction -> !keepers.contains(javaFunction.getMethodName())),
                JavaFunction::getMethodName);

        final String code = gen.generateCode()
                .replace("io.deephaven.plot.FigureImpl", "io.deephaven.plot.Figure");
        log.info("\n\n**************************************\n\n");
        log.info(code);

        String file = devroot + "/Plot/src/main/java/" + OUTPUT_CLASS.replace(".", "/") + ".java";

        if (assertNoChange) {
            String oldCode = new String(Files.readAllBytes(Paths.get(file)));
            GenUtils.assertGeneratedCodeSame(GeneratePlottingConvenience.class, GRADLE_TASK, oldCode, code);
        } else {

            PrintWriter out = new PrintWriter(file);
            out.print(code);
            out.close();

            log.warning(OUTPUT_CLASS + " written to: " + file);
        }
    }

}
