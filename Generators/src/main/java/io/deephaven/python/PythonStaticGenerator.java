/*
 * Copyright (c) 2016-2019 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.python;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;


/**
 * Generates Python file for the public, static method of class(es) determined by the input
 * arguments
 */
public class PythonStaticGenerator {
    private static final List<String> customTableTools =
        Arrays.asList("col", "byteCol", "shortCol", "intCol", "longCol",
            "floatCol", "doubleCol", "charCol", "newTable", "colSource", "objColSource");
    private static final List<String> customParquetTools = Arrays.asList("deleteTable", "readTable",
        "writeParquetTables", "writeTable", "writeTables");
    private static final List<String> customKafkaTools = Arrays.asList(); // "consumeToTable"
    // which methods should just be skipped
    private static final List<String> skipGeneration = Arrays.asList(
        "io.deephaven.db.tables.utils.TableTools,display",
        "io.deephaven.db.tables.utils.DBTimeUtils,convertJimDateTimeQuiet",
        "io.deephaven.db.tables.utils.DBTimeUtils,convertJimMicrosDateTimeQuiet",
        "io.deephaven.db.tables.utils.DBTimeUtils,convertJimMicrosDateTimeQuietFast",
        "io.deephaven.db.tables.utils.DBTimeUtils,convertJimMicrosDateTimeQuietFastTz",
        "io.deephaven.db.tables.utils.DBTimeUtils,diff",
        "io.deephaven.db.tables.utils.DBTimeUtils,yearDiff",
        "io.deephaven.db.tables.utils.DBTimeUtils,dayDiff",
        "io.deephaven.db.plot.colors.ColorMaps,closureMap",
        "io.deephaven.kafka.KafkaTools,consumeToTable",
        "io.deephaven.kafka.KafkaTools,jsonSpec",
        "io.deephaven.kafka.KafkaTools,avroSpec",
        "io.deephaven.kafka.KafkaTools,simpleSpec",
        "io.deephaven.kafka.KafkaTools,ignoreSpec");
    private static final List<String> skipClassDocs = Collections.emptyList();
    private static final Logger log = Logger.getLogger(PythonStaticGenerator.class.toString());
    private static final String gradleTask = ":Generators:generatePythonIntegrationStaticMethods";
    private static List<PythonGeneratorParser.DocstringContainer> classDocContainer;

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        PythonGeneratorParser.validateArgs(args, log);
        final String devroot = args[0];
        final boolean assertNoChange = Boolean.parseBoolean(args[1]);
        final List<String> docRoot = PythonGeneratorParser.getDefaultDocRoot(devroot);

        log.info("PythonStaticGenerator - using system file encoding: "
            + System.getProperty("file.encoding"));

        PythonGeneratorParser.GeneratorElement[] elements =
            PythonGeneratorParser.parse(args, devroot);
        for (PythonGeneratorParser.GeneratorElement element : elements) {
            final String javaClass = element.getClassString();
            final String preambleFilePath = element.getPreambleFile();
            final String destinationFilePath = element.getDestinationFile();
            classDocContainer =
                PythonGeneratorParser.getDocstringContainer(javaClass, docRoot, log);

            PythonGeneratorParser.logGenerationMessage(log, javaClass, preambleFilePath,
                destinationFilePath);

            // create the code container
            final StringBuilder code = new StringBuilder();

            // fill in the class documentation
            if (!skipClassDocs.contains(javaClass)) {
                final String classDoc =
                    PythonGeneratorParser.getClassDocstring(classDocContainer, 0);
                code.append(classDoc).append("\n\n");
            }

            // fill in the preamble
            if (preambleFilePath != null) {
                final byte[] encoded = Files.readAllBytes(Paths.get(preambleFilePath));
                code.append(new String(encoded, StandardCharsets.UTF_8));
            } else {
                makeGenericPreamble(code, javaClass);
            }
            final List<String> generatedMethods = new ArrayList<>();

            // find all the methods
            final PythonGeneratorParser.MethodContainer methodContainer =
                PythonGeneratorParser.getPublicStaticMethodsDetails(javaClass, skipGeneration);
            for (final PythonGeneratorParser.MethodSignatureCollection method : methodContainer
                .getMethodSignatures().values()) {
                final String methodName = method.getMethodName();
                // get digested method signature
                final PythonGeneratorParser.MethodSignature methodDigest = method.reduceSignature();
                if (!skipGeneration.contains(javaClass + "," + methodName)) {
                    createMethod(javaClass, methodName, methodDigest, generatedMethods);
                }
            }

            // add in all of the generated methods
            code.append(String.join("\n\n", generatedMethods));
            PythonGeneratorParser.finalizeGeneration(code, assertNoChange, destinationFilePath,
                javaClass, gradleTask);
        }
    }

    private static void createMethod(String javaClass, String methodName,
        PythonGeneratorParser.MethodSignature methodSig,
        final List<String> generatedMethods) {
        final String paramString = methodSig.createPythonParams();
        final Class rClass = methodSig.getReturnClass();

        final String beginMethod = "@_passThrough\ndef " + methodName + "(" + paramString + "):" +
            PythonGeneratorParser.getMethodDocstring(classDocContainer, methodName, 4) + "\n";
        final String endMethod;
        if ((javaClass.equals("io.deephaven.db.tables.utils.ParquetTools")
            && customParquetTools.contains(methodName)) ||
            (javaClass.equals("io.deephaven.db.tables.utils.TableTools")
                && customTableTools.contains(methodName))
            ||
            (javaClass.equals("io.deephaven.kafka.KafkaTools")
                && customKafkaTools.contains(methodName))) {
            endMethod = "    return _custom_" + methodName + "(" + paramString + ")\n";
        } else if ((rClass != null) && (rClass.isArray())) {
            endMethod = "    return list(_java_type_." + methodName + "(" + paramString + "))\n";
        } else {
            endMethod = "    return _java_type_." + methodName + "(" + paramString + ")\n";
        }
        generatedMethods.add(beginMethod + endMethod);
    }

    /**
     * Fill in the generic, default preamble
     *
     * @param code code container
     * @param javaClass fully qualified java path
     */
    private static void makeGenericPreamble(final StringBuilder code, final String javaClass) {
        code.append("#\n" +
            "# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending\n" +
            "#\n" +
            "\n" +
            "##############################################################################\n" +
            "# This code is auto generated. DO NOT EDIT FILE!\n" +
            "# Run \"./gradlew " + gradleTask + "\" to generate\n" +
            "##############################################################################\n" +
            "\n" +
            "\n" +
            "import jpy\n" +
            "import wrapt\n" +
            "\n" +
            "\n" +
            "_java_type_ = None  # None until the first _defineSymbols() call\n" +
            "\n" +
            "\n" +
            "def _defineSymbols():\n" +
            "    \"\"\"\n" +
            "    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,\n"
            +
            "    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an\n"
            +
            "    exception if the jvm wasn't initialized BEFORE importing the module.\n" +
            "    \"\"\"\n\n" +
            "    if not jpy.has_jvm():\n" +
            "        raise SystemError(\"No java functionality can be used until the JVM has been initialized through the jpy module\")\n"
            +
            "\n" +
            "    global _java_type_\n" +
            "    if _java_type_ is None:\n" +
            "        # This will raise an exception if the desired object is not the classpath\n" +
            "        _java_type_ = jpy.get_type(\"" + javaClass + "\")\n" +
            "\n" +
            "\n" +
            "# every module method should be decorated with @_passThrough\n" +
            "@wrapt.decorator\n" +
            "def _passThrough(wrapped, instance, args, kwargs):\n" +
            "    \"\"\"\n" +
            "    For decoration of module methods, to define necessary symbols at runtime\n\n" +
            "    :param wrapped: the method to be decorated\n" +
            "    :param instance: the object to which the wrapped function was bound when it was called\n"
            +
            "    :param args: the argument list for `wrapped`\n" +
            "    :param kwargs: the keyword argument dictionary for `wrapped`\n" +
            "    :return: the decorated version of the method\n" +
            "    \"\"\"\n\n" +
            "    _defineSymbols()\n" +
            "    return wrapped(*args, **kwargs)\n" +
            "\n\n" +
            "# Define all of our functionality, if currently possible\n" +
            "try:\n" +
            "    _defineSymbols()\n" +
            "except Exception as e:\n" +
            "    pass\n" +
            "\n\n");
    }

}
