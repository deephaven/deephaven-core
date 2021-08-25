package io.deephaven.db.v2.utils;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.libs.QueryLibrary;
import junit.framework.TestCase;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ModelFileGenerator {
    private final Class classType;

    public ModelFileGenerator(Class classType) {
        this.classType = classType;
    }

    private String getPath() {
        return Configuration.getInstance().getProperty("devroot") +
            "/DB/src/test/java/" + classType.getCanonicalName().replace('.', '/') + ".java";
    }

    public void generateFile(final String rawClassDef) throws FileNotFoundException {
        final String processedClassDef = processClassDef(rawClassDef);
        try (final PrintStream out = new PrintStream(new FileOutputStream(getPath()))) {
            out.print(processedClassDef);
        }
    }

    public void validateFile(final String rawClassDef) throws IOException {
        final String processedClassDef = processClassDef(rawClassDef);
        final byte[] encoded = Files.readAllBytes(Paths.get(getPath()));
        final String currentVersion = new String(encoded);
        TestCase.assertEquals(
            "Code generation results have changed - if you are comfortable with the change, run generateFile above to update the reference implementation",
            currentVersion, processedClassDef);
    }

    private String processClassDef(final String rawClassDef) {
        return "package io.deephaven.db.v2.select;\n" +
            rawClassDef.replace("$CLASSNAME$", classType.getSimpleName())
                .replace("import static io.deephaven.numerics.suanshu.SuanShuIntegration.*;", "");

    }
}
