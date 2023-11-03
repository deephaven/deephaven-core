/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.gen;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract class for generating Java code from public static methods.
 */
public abstract class AbstractPublicStaticGenerator {
    private static final Logger log = Logger.getLogger(AbstractPublicStaticGenerator.class.toString());

    private final Map<JavaFunction, JavaFunction> staticFunctions = new TreeMap<>();
    private final Collection<Predicate<JavaFunction>> skips;

    /**
     * Constructor.
     *
     * @param imports Array of fully qualified class names to process.
     * @param skips   Collection of predicates to determine if a function should be skipped.
     * @throws ClassNotFoundException If a class in the imports array cannot be found.
     */
    public AbstractPublicStaticGenerator(final String[] imports, Collection<Predicate<JavaFunction>> skips)
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

    /**
     * Get the public static functions.
     *
     * @return The public static functions.
     */
    public Map<JavaFunction, JavaFunction> getStaticFunctions() {
        return staticFunctions;
    }

    /**
     * Get the skip predicates.
     *
     * @return The skip predicates.
     */
    public Collection<Predicate<JavaFunction>> getSkips() {
        return skips;
    }

    /**
     * Generate the code.
     *
     * @return The generated code.
     */
    abstract public String generateCode();

    /**
     * Run a generator from the command line.
     *
     * @param gen              The generator to run.
     * @param relativeFilePath The relative file path to write the generated code to.
     * @param gradleTask       The gradle task to run the generator.
     * @param args             The command line arguments.
     * @throws IOException            If there is an IO error.
     */
    public static void runCommandLine(final AbstractPublicStaticGenerator gen, final String relativeFilePath, final String gradleTask, final String[] args) throws IOException {

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
        log.warning("Running " + gen.getClass().getSimpleName() + " assertNoChange=" + assertNoChange);

        final String code = gen.generateCode();
        log.info("\n\n**************************************\n\n");
        log.info(code);

        String file = devroot + relativeFilePath;

        if (assertNoChange) {
            String oldCode = new String(Files.readAllBytes(Paths.get(file)));
            GenUtils.assertGeneratedCodeSame(AbstractPublicStaticGenerator.class, gradleTask, oldCode, code);
        } else {

            PrintWriter out = new PrintWriter(file);
            out.print(code);
            out.close();

            log.warning("Class file written to: " + file);
        }
    }

}
