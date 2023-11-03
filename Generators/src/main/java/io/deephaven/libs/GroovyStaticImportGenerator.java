/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.libs;

import io.deephaven.gen.GenUtils;
import io.deephaven.gen.JavaFunction;

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
    private static final String GRADLE_TASK = ":Generators:groovyStaticImportGenerator";


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

    private String generateCode() {

        String code = GenUtils.javaHeader(this.getClass(), GRADLE_TASK);
        code += "package io.deephaven.libs;\n\n";

        Set<String> imports = GenUtils.typesToImport(staticFunctions.keySet());

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

            final String javadoc = "    /** @see " + f.getClassName() + "#" + f.getMethodName() + "(" +
                    Arrays.stream(f.getParameterTypes()).map(GenUtils::getParamTypeString)
                            .collect(Collectors.joining(","))
                    +
                    ") */";
            final String sigPrefix = "public static";
            final String callArgs = GenUtils.argString(f, false);
            final String funcBody = " {return " + f.getClassNameShort() + "." + f.getMethodName() + "(" + callArgs + " );}\n";
            code += GenUtils.createFunction(f, sigPrefix, javadoc, funcBody);
            code += "\n";
        }

        code += "}\n\n";

        return code;
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
            GenUtils.assertGeneratedCodeSame(GroovyStaticImportGenerator.class, GRADLE_TASK, oldCode, code);
        } else {

            PrintWriter out = new PrintWriter(file);
            out.print(code);
            out.close();

            log.warning("io.deephaven.libs.GroovyStaticImports.java written to: " + file);
        }
    }

}
