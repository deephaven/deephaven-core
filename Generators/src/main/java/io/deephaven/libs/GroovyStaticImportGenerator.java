//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.libs;

import io.deephaven.gen.AbstractBasicJavaGenerator;
import io.deephaven.gen.GenUtils;
import io.deephaven.gen.JavaFunction;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;


/**
 * Groovy has a bug where performing a static import on multiple libraries containing functions with the same name
 * causes some of the functions to not be present in the namespace. This class combines static imports from multiple
 * sources into a single class that can be imported.
 */
public class GroovyStaticImportGenerator extends AbstractBasicJavaGenerator {

    public GroovyStaticImportGenerator(String gradleTask, String packageName, String className, String[] imports,
            Predicate<Method> includeMethod, Collection<Predicate<JavaFunction>> skipsGen, Level logLevel)
            throws ClassNotFoundException {
        super(gradleTask, packageName, className, imports, includeMethod, skipsGen, logLevel);
    }

    @Override
    public String generateClassJavadoc() {
        String code = "";
        code += "/**\n";
        code += " * Functions statically imported into Groovy.\n";
        code += " *\n";
        code += " * @see io.deephaven.function\n";
        code += " */\n";
        return code;
    }

    @Override
    public String generateFunction(JavaFunction f) {
        final String javadoc = "    /** @see " + f.getClassName() + "#" + f.getMethodName() + "(" +
                Arrays.stream(f.getParameterTypes()).map(GenUtils::javadocLinkParamTypeString)
                        .collect(Collectors.joining(","))
                +
                ") */";
        final String sigPrefix = "public static";
        final String callArgs = GenUtils.javaArgString(f, false);
        final String funcBody =
                " {return " + f.getClassNameShort() + "." + f.getMethodName() + "(" + callArgs + " );}\n";
        return GenUtils.javaFunction(f, sigPrefix, javadoc, funcBody);
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        final String gradleTask = ":Generators:groovyStaticImportGenerator";
        final String packageName = "io.deephaven.libs";
        final String className = "GroovyStaticImports";

        final String relativeFilePath = "/engine/table/src/main/java/io/deephaven/libs/GroovyStaticImports.java";

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

        GroovyStaticImportGenerator gen = new GroovyStaticImportGenerator(gradleTask, packageName, className, imports,
                (m) -> Modifier.isStatic(m.getModifiers()) && Modifier.isPublic(m.getModifiers()),
                // skipping common erasure "sum"
                Collections.singletonList((f) -> f.getMethodName().equals("sum") && f.getParameterTypes().length == 1
                        && f.getParameterTypes()[0].getTypeName()
                                .contains("ObjectVector<")),
                Level.WARNING);

        runCommandLine(gen, relativeFilePath, args);
    }

}
