package io.deephaven.demo.deploy;


import org.apache.commons.io.FileUtils;
import org.jboss.logging.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 *
 */
public class Execute {

    private static final Logger LOG = Logger.getLogger(Execute.class);

    static Consumer<CharSequence> DO_NOTHING = c -> {
    };
    static ThreadLocal<Boolean> quietMode = new ThreadLocal<>();

    static Map<String, String> getSshEnvMap() {
        final Map<String, String> map = new HashMap<>();
        map.put("SSH_AUTH_SOCK", System.getenv("SSH_AUTH_SOCK"));
        map.put("PATH", System.getenv("PATH"));
        return map;
    }

    public static class ExecutionResult {
        public int code;
        public String out;
        public String err;
        public String sourceCode;

        public ExecutionResult(final String sourceCode) {
            this.sourceCode = sourceCode;
        }

        public String getSourceCode() {
            return sourceCode;
        }

        @Override
        public String toString() {
            return "ExecutionResult{" +
                    "resultCode=" + code +
                    ", sourceCode='" + sourceCode + '\'' +
                    ", out='" + out + '\'' +
                    ", err='" + err + '\'' +
                    '}';
        }
    }

    static Consumer<CharSequence> toLogger(PrintStream stream) {
        return stream::println;
    }

    static ExecutionResult executeNoFail(String cmd, Map<String, String> env, File workDir, File saveTo, Consumer<CharSequence> logOut, Consumer<CharSequence> logErr) throws IOException, InterruptedException {
        if (env == null) {
            env = new HashMap<>();
        }
        ExecutionResult result = execute(cmd, env, workDir, saveTo, logOut, logErr);
        if (result.code != 0) {
            throw new IllegalStateException("Execution failed on command: $cmd");
        }
        return result;
    }

    static ExecutionResult bashQuiet(String name, String code) throws IOException, InterruptedException {
        return bash(name, code, new HashMap<>(),null, null, DO_NOTHING, DO_NOTHING);
    }

    static ExecutionResult bash(String name, String code, Map<String, String> env, File workDir, File saveTo, Consumer<CharSequence> logOut,Consumer<CharSequence>logErr) throws IOException, InterruptedException {
        return execute(Arrays.asList("bash", "-c", code.startsWith("function ") ? code : "function " + name + "() {\n"+
code +
"\n} ; " + name),env, workDir, saveTo, logOut, logErr);
    }

    static ExecutionResult executeNoFail(String ... cmd) throws IOException, InterruptedException {
        return executeNoFail(Arrays.asList(cmd));
    }
    static ExecutionResult executeNoFail(List<String> cmd) throws IOException, InterruptedException {
        return executeNoFail(cmd, new HashMap<>());
    }
    static ExecutionResult executeNoFail(List<String> cmd, Map<String, String> env) throws IOException, InterruptedException {
        return executeNoFail(cmd, env, null, null, null, null);
    }
    static ExecutionResult executeNoFail(List<String> cmd, Map<String, String> env, File workDir, File saveTo, Consumer<CharSequence> logOut,Consumer<CharSequence>logErr) throws IOException, InterruptedException {
        ExecutionResult result = execute(cmd, env, workDir, saveTo, logOut, logErr);
        if (result.code != 0) {
            throw new IllegalStateException("Execution failed on command: \"" + cmd.stream().collect(Collectors.joining("\" \"")) + "\"\n" +
"stdout: \n"+
result.out + "\n" +
"stderr: \n" +
result.err);
        }
        return result;
    }

    public static ExecutionResult execute(String ... cmd) throws IOException, InterruptedException {
        return execute(Arrays.asList(cmd));
    }
    public static ExecutionResult executeQuiet(String ... cmd) throws IOException, InterruptedException {
        return execute(Arrays.asList(cmd), null, null, null, DO_NOTHING, DO_NOTHING);
    }
    public static ExecutionResult execute(List<String> cmd) throws IOException, InterruptedException {
        return execute(cmd, new HashMap<>());
    }
    public static ExecutionResult executeQuiet(List<String> cmd) throws IOException, InterruptedException {
        return execute(cmd, null, null, null, DO_NOTHING, DO_NOTHING);
    }
    public static ExecutionResult execute(List<String> cmd, Map<String, String> env) throws IOException, InterruptedException {
        return execute(cmd, env, null, null, null, null);
    }
    public static ExecutionResult execute(List<String> cmd, Map<String, String> env, File workDir, File saveTo, Consumer<CharSequence> logOut, Consumer<CharSequence> logErr) throws IOException, InterruptedException {
        if (env == null) {
            env = new HashMap<>();
        }
        boolean quiet = Boolean.TRUE.equals(quietMode.get());
        if (logOut == null) {
            logOut = quiet ? DO_NOTHING : toLogger(System.out);
        }
        if (logErr == null) {
            logErr = quiet ? DO_NOTHING : toLogger(System.err);
        }
        if (cmd.stream().anyMatch(Objects::isNull)) {
            String msg = "Cannot pass null arguments: " + cmd;
            System.out.println(msg);
            throw new NullPointerException(msg);
        }

        String niceForm = "\"" + cmd.stream().collect(Collectors.joining("\" \"")) + "\"";
        if (logOut != DO_NOTHING && logErr != DO_NOTHING) {
            LOG.info("Executing command: " + niceForm + " in directory " + (workDir != null ? workDir : new File(".").getAbsolutePath()));
        }
        if (saveTo != null) {
            FileUtils.write(saveTo, niceForm, StandardCharsets.UTF_8, false);
        }

        Process proc = exec(cmd, env, workDir);
        return getResult(niceForm, proc, logOut, logErr);
    }

    private static Process exec(final List<String> cmd, final Map<String, String> env, final File workDir) throws IOException {
        final ProcessBuilder proc = new ProcessBuilder(cmd);
        return exec(proc, env, workDir);
    }

    private static Process exec(final String cmd, final Map<String, String> env, final File workDir) throws IOException {
        final ProcessBuilder proc = new ProcessBuilder(cmd);
        return exec(proc, env, workDir);
    }

    private static Process exec(final ProcessBuilder proc, final Map<String, String> env, final File workDir) throws IOException {
        if (workDir != null) {
            proc.directory(workDir);
        }
        if (env != null) {
            proc.environment().putAll(env);
        }
        return proc.start();
    }

    static ExecutionResult execute(String cmd, Map<String, String> env) throws IOException, InterruptedException {
        return execute(cmd, env, null, null, null, null);
    }
    static ExecutionResult execute(String cmd, Map<String, String> env, File workDir, File saveTo, Consumer<CharSequence> logOut, Consumer<CharSequence> logErr) throws IOException, InterruptedException {
        System.out.println("Executing deployment command: " + cmd + " in directory " + (workDir != null ? workDir : new File(".").getAbsolutePath()));
        if (saveTo != null) {
            FileUtils.write(saveTo, cmd + "\n", StandardCharsets.UTF_8);
        }

        Process proc = exec(cmd, env, workDir);
        return getResult(cmd, proc, logOut, logErr);
    }

    private static List<Process> runningProc;

    private static ExecutionResult getResult(String sourceCode, Process proc, Consumer<CharSequence> logOut, Consumer<CharSequence> logErr) throws InterruptedException {
        synchronized (Execute.class) {
            if (runningProc == null) {
                runningProc = new ArrayList<>();
                Runtime.getRuntime().addShutdownHook(
                        new Thread(()-> runningProc.removeIf(p-> {
                            if (p != null) {
                                p.destroyForcibly();
                            }
                            return true;
                        }))
                );
                }
            runningProc.add(proc);
        }
        StringBuffer sout = new StringBuffer(), serr = new StringBuffer();
        Thread soutThread = consumeProcessStream(proc.getInputStream(), logWrap(sout, logOut));
        Thread serrThread = consumeProcessStream(proc.getErrorStream(), logWrap("stderr: ", sout, logErr));

        // for whatever terrible reason, using waitFor() w/ a timeout nets us an exit code 137 (kill -9),
        // but a plain, block-forever .waitFor() gets us exit code 0
        // TODO: move this waitFor into a bg thread that we can block on w/ a timeout.
        proc.waitFor();
        // TODO: take a Duration argument?
        if (!proc.waitFor(60, TimeUnit.SECONDS)) {
            System.err.println("Waited more than 60 seconds to run process " + sourceCode);
        }
        ExecutionResult result = new ExecutionResult(sourceCode);
        result.code = proc.exitValue();
        // make sure we wait for our output-consuming threads to join, or else we might truncate output!
        soutThread.join();
        result.out = sout.toString();
        serrThread.join();
        result.err = serr.toString();
        runningProc.remove(proc);
        return result;
    }

    private static Thread consumeProcessStream(final InputStream stream, final Appendable logWrap) {
        Thread thread = new Thread(new TextDumper(stream, logWrap));
        thread.start();
        return thread;
    }

    public static ExecutionResult ssh(boolean canFail, String machine, String... command) throws IOException, InterruptedException {
        if (!machine.contains("@")) {
            String sshUser = System.getProperty("sshUser", "");
            if (!sshUser.isEmpty()) {
                machine = sshUser + "@" + machine;
            }
        }
        List<String> args = new ArrayList<>(Arrays.asList("ssh", "-i", getAdminSshKey(),
                "-o", "IdentitiesOnly=yes",
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "RequestTTY=no",
                "-o", "BatchMode=yes",
                "-o", "ServerAliveInterval=10",
//                "-o", "PermitLocalCommand=yes",
//                "-o", "LocalCommand=echo local done",
//                "-o", "LogLevel=DEBUG2",
                machine));
        args.addAll(Arrays.asList(command));
        return canFail ? execute(args, getSshEnvMap()) : executeNoFail(args, getSshEnvMap());
    }

    static String getAdminUser() {
        String admin = System.getenv("DH_SYSADMIN_USER");
        if (admin == null) {
            admin = System.getProperty("user.name");
        }
        return admin;
    }

    static String getAdminSshKey() {
        String adminKey = System.getenv("DH_ADMIN_SSH_KEY");
        if (adminKey == null) {
            adminKey = System.getProperty("DH_ADMIN_SSH_KEY");
        }
        if (adminKey == null) {
            String userHome = System.getProperty("user.home", "~");
            adminKey = userHome + "/.ssh/google_compute_engine";
        }
        return adminKey;
    }

    static ExecutionResult sshQuiet(String machine, boolean allowFail, String... command) throws IOException, InterruptedException {
        if (!machine.contains("@")) {
            machine = getAdminUser() + "@" + machine;
        }
        List<String> args = new ArrayList<>();
        args.addAll(Arrays.asList(
        "ssh", "-i", getAdminSshKey(),
                "-o", "IdentitiesOnly=yes",
                "-o", "StrictHostKeyChecking=no",
                "-o", "ConnectTimeout=10",
                "-o", "UserKnownHostsFile=/dev/null", machine
        ));
        args.addAll(Arrays.asList(command));
        return allowFail ? execute(args, getSshEnvMap(), null, null, DO_NOTHING, DO_NOTHING) : executeNoFail(args, getSshEnvMap(), null, null, DO_NOTHING, DO_NOTHING);
    }

    static Appendable logWrap(StringBuffer out, Consumer<CharSequence> log) {
        return logWrap("", out, log);
    }
    static Appendable logWrap(String logPrefix, StringBuffer out, Consumer<CharSequence> log) {
        return new Appendable() {
            // annoying trick to convince Groovy to let us reference `this` in anonymous
            private Appendable me = this;

            @Override
            public Appendable append(CharSequence csq) throws IOException {
                out.append(csq);
                if (!csq.toString().trim().isEmpty()) {
                    log.accept(logPrefix + " " + csq);
                }
                return this.me;
            }

            @Override
            public Appendable append(CharSequence csq, int start, int end) throws IOException {
                out.append(csq, start, end);
                CharSequence sub = csq.subSequence(start, end);
                if (!sub.toString().trim().isEmpty()) {
                    log.accept(logPrefix + " " + sub);
                }
                return this.me;
            }

            @Override
            public Appendable append(char c) throws IOException {
                out.append(c);
                if (!Character.isWhitespace(c)) {
                    log.accept(logPrefix + " " + c);
                }
                return this.me;
            }
        };
    }
}

class TextDumper implements Runnable {
    final InputStream in;
    final Appendable app;

    public TextDumper(InputStream in, Appendable app) {
        this.in = in;
        this.app = app;
    }

    public void run() {
        InputStreamReader isr = new InputStreamReader(this.in);
        BufferedReader br = new BufferedReader(isr);

        try {
            String next;
            while((next = br.readLine()) != null) {
                if (this.app != null) {
                    this.app.append(next);
                    if (!next.isEmpty()) {
                        this.app.append("\n");
                    }
                }
            }

        } catch (IOException var5) {
            throw new RuntimeException("exception while reading process stream", var5);
        }
    }
}
