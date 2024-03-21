//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.gen;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract class for generating Java code from methods of one or more classes.
 *
 * {@link #generateCode()} must be called to generate the code.
 */
public abstract class AbstractBasicJavaGenerator {
    private static final Logger log = Logger.getLogger(AbstractBasicJavaGenerator.class.toString());

    private final String gradleTask;
    private final String packageName;
    private final String className;
    private final Map<JavaFunction, JavaFunction> functions = new TreeMap<>();
    private final Collection<Predicate<JavaFunction>> skipsGen;

    /**
     * Constructor.
     *
     * @param gradleTask Gradle task to generate the code.
     * @param packageName Package name for the generated class.
     * @param className Class name for the generated class.
     * @param imports Array of fully qualified class names to process.
     * @param includeMethod a predicate to determine if a method should be considered for code generation.
     * @param skipsGen Collection of predicates to determine if a function should be skipped when generating code.
     * @throws ClassNotFoundException If a class in the imports array cannot be found.
     */
    public AbstractBasicJavaGenerator(
            final String gradleTask,
            final String packageName,
            final String className,
            final String[] imports,
            final Predicate<Method> includeMethod,
            final Collection<Predicate<JavaFunction>> skipsGen,
            final Level logLevel)
            throws ClassNotFoundException {
        this.gradleTask = gradleTask;
        this.packageName = packageName;
        this.className = className;
        this.skipsGen = skipsGen;

        log.setLevel(logLevel);

        for (String imp : imports) {
            Class<?> c = Class.forName(imp, false, Thread.currentThread().getContextClassLoader());
            log.info("Processing class: " + c);

            for (Method m : c.getMethods()) {
                log.info("Processing method (" + c + "): " + m);
                if (includeMethod.test(m)) {
                    addMethod(m);
                }
            }
        }
    }

    private void addMethod(Method m) {
        log.info("Processing public static method: " + m);

        JavaFunction f = new JavaFunction(m);
        // System.out.println(f);

        if (functions.containsKey(f)) {
            JavaFunction fAlready = functions.get(f);
            final String message = "Signature Already Present:	" + fAlready + "\t" + f;
            log.severe(message);
            throw new RuntimeException(message);
        } else {
            log.info("Added method: " + f);
            functions.put(f, f);
        }
    }

    /**
     * Generate Javadoc for the output class.
     *
     * @return The Javadoc.
     */
    abstract public String generateClassJavadoc();

    /**
     * Generate the Java code for a function.
     *
     * @param f The function.
     * @return The Java code.
     */
    abstract public String generateFunction(JavaFunction f);

    /**
     * Generate the code.
     *
     * @return The generated code.
     */
    @SuppressWarnings("StringConcatenationInLoop")
    public String generateCode() {

        String code = GenUtils.javaHeader(this.getClass(), gradleTask);
        code += "package " + packageName + ";\n\n";

        Set<String> imports = GenUtils.typesToImport(functions.keySet());

        for (String imp : imports) {
            code += "import " + imp + ";\n";
        }

        code += "\n";
        code += generateClassJavadoc();
        code += "public class " + className + " {\n";

        for (JavaFunction f : functions.keySet()) {
            boolean skip = false;
            for (Predicate<JavaFunction> skipCheck : skipsGen) {
                skip = skip || skipCheck.test(f);
            }

            if (skip) {
                log.warning("*** Skipping function: " + f);
                continue;
            }

            code += generateFunction(f);
            code += "\n";
        }

        code += "}\n\n";

        return code;
    }

    /**
     * Run a generator from the command line.
     *
     * @param gen The generator to run.
     * @param relativeFilePath The relative file path to write the generated code to.
     * @param args The command line arguments.
     * @throws IOException If there is an IO error.
     */
    public static void runCommandLine(final AbstractBasicJavaGenerator gen, final String relativeFilePath,
            final String[] args) throws IOException {

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

        log.warning("Running " + gen.getClass().getSimpleName() + " assertNoChange=" + assertNoChange);

        final String code = gen.generateCode();
        log.info("\n\n**************************************\n\n");
        log.info(code);

        String file = devroot + relativeFilePath;

        if (assertNoChange) {
            String oldCode = new String(Files.readAllBytes(Paths.get(file)));
            GenUtils.assertGeneratedCodeSame(AbstractBasicJavaGenerator.class, gen.gradleTask, oldCode, code);
        } else {

            PrintWriter out = new PrintWriter(file);
            out.print(code);
            out.close();

            log.warning("Class file written to: " + file);
        }
    }

}
