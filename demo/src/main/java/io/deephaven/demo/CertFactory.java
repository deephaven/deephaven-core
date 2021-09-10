package io.deephaven.demo;

import com.google.common.io.CharSink;
import com.google.common.io.Files;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * CertFactory:
 * <p>
 * <p>
 * Created by James X. Nelson (James@WeTheInter.net) on 08/08/2021 @ 1:11 a.m..
 */
public class CertFactory {

    public static void main(String... args) throws IOException {

        // load up our cert generating shell script
        final String genKeyPath = "/scripts/gen-certs.sh";
        final InputStream genKeyScript = CertFactory.class.getResourceAsStream(genKeyPath);
        System.out
            .println("Prop test: " + System.getProperty("quarkus.http.ssl.certificate.key-file"));
        if (genKeyScript == null) {
            System.err.println("No " + genKeyPath + " found in classloader, bailing!");
            System.exit(98);
        }
        String tmpDir = System.getenv("AUTH_DIR");
        if (tmpDir == null) {
            tmpDir = "/tmp";
        }
        String scriptLoc = tmpDir + "/gen-certs.sh";
        final File scriptFile = new File(scriptLoc);
        final CharSink dest = Files.asCharSink(scriptFile, StandardCharsets.UTF_8);
        dest.writeFrom(new InputStreamReader(genKeyScript));
        scriptFile.setExecutable(true);

        // There, now the file exists. run it.
        ProcessBuilder proc = new ProcessBuilder();
        proc.command("bash", scriptLoc);
        proc.inheritIO();
        final Process run = proc.start();
        try {
            if (run.waitFor(20, TimeUnit.SECONDS)) {
                System.exit(run.exitValue());
            }
            System.err.println("gen-certs.sh did not complete in 20s, killing process");
        } catch (InterruptedException e) {
            System.err.println("gen-certs.sh was interrupted, killing process");
        }
        run.destroy();
        System.exit(97);
    }
}
