/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.plot.FigureImpl;
import io.deephaven.libs.GroovyStaticImportGenerator.JavaFunction;
import java.lang.reflect.WildcardType;

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
 * Create static functions that resolve against the last created instance of a plotting figure class. This is to make a
 * cleaner plotting interface
 */
public class GeneratePlottingConvenience {
    // See also GroovyStaticImportGenerator

    private static final Logger log = Logger.getLogger(GeneratePlottingConvenience.class.toString());

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
            final Class<?> c = Class.forName(classPath);
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
            final Class<?> c = Class.forName(imp);
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
        final JavaFunction signature = new JavaFunction(
                OUTPUT_CLASS,
                OUTPUT_CLASS_NAME_SHORT,
                functionNamer.apply(f),
                f.getTypeParameters(),
                f.getReturnType(),
                f.getParameterTypes(),
                f.getParameterNames(),
                f.isVarArgs());

        boolean skip = skip(f, ignoreSkips);

        if (skip) {
            log.warning("*** Skipping function: " + f);
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
        } else if (t instanceof WildcardType) {
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

    private String generateCode() {

        String code = "/*\n" +
                " * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending\n" +
                " */\n\n" +
                "/****************************************************************************************************************************\n"
                +
                " ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - Run GeneratePlottingConvenience or \"./gradlew :Generators:generatePlottingConvenience\" to regenerate\n"
                +
                " ****************************************************************************************************************************/\n\n";

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
                log.warning("*** Skipping static function: " + f);
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
                log.warning("*** Skipping function: " + f);
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
        String returnType = f.getReturnType().getTypeName().replace("$", ".");
        String s = createJavadoc(f, signature);
        s += "    public static ";

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

        s += " " + returnType + " " + signature.getMethodName() + "(";
        String callArgs = "";

        for (int i = 0; i < f.getParameterTypes().length; i++) {
            if (i != 0) {
                s += ",";
                callArgs += ",";
            }

            Type t = f.getParameterTypes()[i];

            String typeString = t.getTypeName().replace("$", ".");

            if (f.isVarArgs() && i == f.getParameterTypes().length - 1) {
                final int index = typeString.lastIndexOf("[]");
                typeString = typeString.substring(0, index) + "..." + typeString.substring(index + 2);
            }

            s += " " + typeString + " " + f.getParameterNames()[i];
            callArgs += " " + f.getParameterNames()[i];
        }

        s += " ) {\n";
        s += indent(2) + (f.getReturnType().equals(void.class) ? "" : "return ")
                + (isStatic ? (f.getClassNameShort() + ".") : "FigureFactory.figure().") + f.getMethodName() + "("
                + callArgs + " );\n";
        s += indent(1) + "}\n";

        return s;
    }

    private static String createJavadoc(final JavaFunction f, final JavaFunction signature) {
        return "    /**\n    * See {@link " + f.getClassName() + "#" + signature.getMethodName() + "} \n    **/\n";
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
                "catErrorBarBy"));

        @SuppressWarnings("unchecked")
        GeneratePlottingConvenience gen = new GeneratePlottingConvenience(staticImports, imports,
                Arrays.asList(javaFunction -> !keepers.contains(javaFunction.getMethodName())),
                JavaFunction::getMethodName);

        final String code = gen.generateCode()
                .replace("io.deephaven.plot.FigureImpl", "io.deephaven.plot.Figure");
        log.info("\n\n**************************************\n\n");
        log.info(code);

        String file = devroot + "/Plot/src/main/java/" + OUTPUT_CLASS.replace(".", "/") + ".java";

        if (assertNoChange) {
            String oldCode = new String(Files.readAllBytes(Paths.get(file)));
            if (!code.equals(oldCode)) {
                throw new RuntimeException(
                        "Change in generated code.  Run GeneratePlottingConvenience or \"./gradlew :Generators:generatePlottingConvenience\" to regenerate\n");
            }
        } else {

            PrintWriter out = new PrintWriter(file);
            out.print(code);
            out.close();

            log.warning(OUTPUT_CLASS + " written to: " + file);
        }
    }

}
