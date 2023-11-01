/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.libs;

import io.deephaven.gen.JavaFunction;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static io.deephaven.gen.GenUtils.typesToImport;


/**
 * Groovy has a bug where performing a static import on multiple libraries containing functions with the same name
 * causes some of the functions to not be present in the namespace. This class combines static imports from multiple
 * sources into a single class that can be imported.
 */
@SuppressWarnings("StringConcatenationInLoop")
public class GroovyStaticImportGenerator {
    private static final Logger log = Logger.getLogger(GroovyStaticImportGenerator.class.toString());


    private final Map<JavaFunction, JavaFunction> staticFunctions = new TreeMap<>();
    private final Collection<Predicate<JavaFunction>> skips;

    private GroovyStaticImportGenerator(final String[] imports, Collection<Predicate<JavaFunction>> skips)
            throws ClassNotFoundException {
        this.skips = skips;

        for (String imp : imports) {
            Class<?> c = Class.forName(imp, false, Thread.currentThread().getContextClassLoader());
            log.info("Processing class: " + c);

            for (Method m : c.getMethods()) {
                log.info("Processing method (" + c + "): " + m);
                boolean isStatic = Modifier.isStatic(m.getModifiers());
                boolean isPublic = Modifier.isPublic(m.getModifiers());

                if (isStatic && isPublic) {
                    addPublicStatic(m);
                }
            }
        }
    }

    private void addPublicStatic(Method m) {
        log.info("Processing public static method: " + m);

        JavaFunction f = new JavaFunction(m);
        // System.out.println(f);

        if (staticFunctions.containsKey(f)) {
            JavaFunction fAlready = staticFunctions.get(f);
            final String message = "Signature Already Present:	" + fAlready + "\t" + f;
            log.severe(message);
            throw new RuntimeException(message);
        } else {
            log.info("Added public static method: " + f);
            staticFunctions.put(f, f);
        }
    }

    private Set<String> generateImports() {
        Set<String> imports = new TreeSet<>();

        for (JavaFunction f : staticFunctions.keySet()) {
            imports.add(f.getClassName());

            imports.addAll(typesToImport(f.getReturnType()));

            for (Type t : f.getParameterTypes()) {
                imports.addAll(typesToImport(t));
            }
        }

        return imports;
    }

    private String generateCode() {

        String code = "/**\n" +
                " * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending\n" +
                " */\n" +
                "/****************************************************************************************************************************\n"
                +
                " ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - Run GroovyStaticImportGenerator or \"./gradlew :Generators:groovyStaticImportGenerator\" to regenerate\n"
                +
                " ****************************************************************************************************************************/\n\n";

        code += "package io.deephaven.libs;\n\n";

        Set<String> imports = generateImports();

        for (String imp : imports) {
            code += "import " + imp + ";\n";
        }

        code += "\n";
        code += "/**\n";
        code += " * Functions statically imported into Groovy.\n";
        code += " *\n";
        code += " * @see io.deephaven.function\n";
        code += " */\n";
        code += "public class GroovyStaticImports {\n";

        for (JavaFunction f : staticFunctions.keySet()) {
            boolean skip = false;
            for (Predicate<JavaFunction> skipCheck : skips) {
                skip = skip || skipCheck.test(f);
            }

            if (skip) {
                log.warning("*** Skipping function: " + f);
                continue;
            }

            String returnType = f.getReturnType().getTypeName();
            String s =
                    "    /** @see " + f.getClassName() + "#" + f.getMethodName() + "(" +
                            Arrays.stream(f.getParameterTypes()).map(this::getParamTypeString)
                                    .collect(Collectors.joining(","))
                            +
                            ") */\n" +
                            "    public static ";

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

            s += " " + returnType + " " + f.getMethodName() + "(";
            String callArgs = "";

            for (int i = 0; i < f.getParameterTypes().length; i++) {
                if (i != 0) {
                    s += ",";
                    callArgs += ",";
                }

                Type t = f.getParameterTypes()[i];

                s += " " + t.getTypeName() + " " + f.getParameterNames()[i];
                callArgs += " " + f.getParameterNames()[i];
            }

            s += " ) {";
            s += "return " + f.getClassNameShort() + "." + f.getMethodName() + "(" + callArgs + " );";
            s += "}";

            code += s;
            code += "\n";
        }

        code += "}\n\n";

        return code;
    }

    /**
     * Helper to transform method parameter types to a form that can be used in a javadoc link, including removing
     * generics and finding the upper bound of typevars.
     */
    @NotNull
    private String getParamTypeString(Type t) {
        if (t instanceof ParameterizedType) {
            return ((ParameterizedType) t).getRawType().getTypeName();
        } else if (t instanceof TypeVariable) {
            return getParamTypeString(((TypeVariable<?>) t).getBounds()[0]);
        } else if (t instanceof WildcardType) {
            return getParamTypeString(((WildcardType) t).getUpperBounds()[0]);
        } else if (t instanceof GenericArrayType) {
            return getParamTypeString(((GenericArrayType) t).getGenericComponentType()) + "[]";
        }
        return t.getTypeName();
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
        log.warning("Running GroovyStaticImportGenerator assertNoChange=" + assertNoChange);

        final String[] imports = {
                "io.deephaven.function.Basic",
                "io.deephaven.function.BinSearch",
                // "io.deephaven.function.BinSearchAlgo",
                "io.deephaven.function.Cast",
                "io.deephaven.function.Logic",
                "io.deephaven.function.Numeric",
                "io.deephaven.function.Parse",
                "io.deephaven.function.Random",
                "io.deephaven.function.Sort",
        };

        GroovyStaticImportGenerator gen = new GroovyStaticImportGenerator(imports,
                // skipping common erasure "sum"
                Collections.singletonList((f) -> f.getMethodName().equals("sum") && f.getParameterTypes().length == 1
                        && f.getParameterTypes()[0].getTypeName()
                                .contains("ObjectVector<")));

        final String code = gen.generateCode();
        log.info("\n\n**************************************\n\n");
        log.info(code);

        String file = devroot + "/engine/table/src/main/java/io/deephaven/libs/GroovyStaticImports.java";

        if (assertNoChange) {
            String oldCode = new String(Files.readAllBytes(Paths.get(file)));
            if (!code.equals(oldCode)) {
                throw new RuntimeException(
                        "Change in generated code.  Run GroovyStaticImportGenerator or \"./gradlew :Generators:groovyStaticImportGenerator\" to regenerate\n");
            }
        } else {

            PrintWriter out = new PrintWriter(file);
            out.print(code);
            out.close();

            log.warning("io.deephaven.libs.GroovyStaticImports.java written to: " + file);
        }
    }

}
