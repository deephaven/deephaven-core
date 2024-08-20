//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.generator.primitivetemplate;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.Version;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A template based code generator which can generate code for primitive types.
 */
public class CodeGenerator {

    public static void main(final String[] args) throws Exception {
        System.out.println("CodeGenerator: " + Arrays.toString(args));

        final String templateFile = args[0];
        final String outputFile = args[1];

        Version version = new Version(2, 3, 20);
        Configuration cfg = new Configuration(version);

        Map<String, Object> input = new HashMap<>();
        input.put("primitiveTypes", PrimitiveType.primitiveTypes());

        Template template = cfg.getTemplate(templateFile);

        final Writer fileWriter = new FileWriter(new File(outputFile));

        fileWriter.write(
                String.join("\n",
                        "//",
                        "// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending",
                        "//",
                        "// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY",
                        "// ****** Edit " + templateFile
                                + " and run \"./gradlew :engine-function:compileJava\" to regenerate",
                        "//",
                        "// @formatter:off",
                        ""));

        try {
            template.process(input, fileWriter);
        } finally {
            fileWriter.close();
        }

    }
}
