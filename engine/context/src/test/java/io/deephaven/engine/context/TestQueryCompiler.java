//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.CompletionStageFuture;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class TestQueryCompiler {
    private final static int NUM_THREADS = 500;
    private final static int NUM_METHODS = 5000;
    private final static long WAIT_BETWEEN_THREAD_START_MILLIS = 5;
    private final static long MINIMUM_DELAY_MILLIS = 100;
    private final static int NUM_COMPILE_TESTS = 10;
    private static final String CLASS_CODE;

    private final static List<Throwable> raisedThrowables = new ArrayList<>();

    // Two nearly-identical classes, so we can get an idea of how long it takes to compile one of them
    static {
        final StringBuilder testClassCode1 = new StringBuilder("        public class $CLASSNAME$ {");
        testClassCode1.append("            final static String testString = \"Hello World\\n\";");

        // Simple static inner classes to generate two class files
        testClassCode1.append("            private static class ATestInnerClass {");
        testClassCode1.append("                private int i=1;");
        testClassCode1.append("            }");

        testClassCode1.append("            private static class ZTestInnerClass {");
        testClassCode1.append("                private int i=2;");
        testClassCode1.append("            }");

        for (int i = 0; i < NUM_METHODS; i++) {
            testClassCode1.append("            public static void testMethod").append(i).append(" (String [] args) {");
            testClassCode1.append("                System.out.println(testString);");
            testClassCode1.append("            }");
        }

        testClassCode1.append("        }");
        CLASS_CODE = testClassCode1.toString();
    }

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private SafeCloseable executionContextClosable;

    @Before
    public void setUp() throws IOException {
        executionContextClosable = ExecutionContext.newBuilder()
                .captureQueryLibrary()
                .captureQueryScope()
                .setQueryCompiler(QueryCompilerImpl.create(
                        folder.newFolder(), TestQueryCompiler.class.getClassLoader()))
                .build()
                .open();
    }

    @After
    public void tearDown() {
        executionContextClosable.close();
    }

    @Test
    public void testParallelCompile() throws Throwable {
        // Load Configuration to avoid all that time
        Configuration.getInstance();
        final Thread[] threads = new Thread[NUM_THREADS];

        // Use a unique value added to the class name to guarantee unique classes, in case workspaces aren't cleared out
        // correctly
        final long startTimeOffset = System.currentTimeMillis();

        // Get a baseline estimate of compilation time, ignoring the first run as it's typically much longer
        long totalCompileTimeMillis = 0;
        for (long i = 0; i < NUM_COMPILE_TESTS; i++) {
            final long startTimeTest = System.currentTimeMillis();
            final String testClassName = "TestClass" + startTimeOffset + i;
            compile(false, testClassName);
            final long endTimeTest = System.currentTimeMillis();
            final long compileTestMillis = endTimeTest - startTimeTest;
            System.out.println(printMillis(endTimeTest) + ": compile test of " + testClassName + " took "
                    + compileTestMillis + " millis");
            if (i > 0) {
                totalCompileTimeMillis += compileTestMillis;
            }
        }
        final long averageCompileTime = (totalCompileTimeMillis / (NUM_COMPILE_TESTS - 1));
        final long tempWaitStartMillis = averageCompileTime - 500;
        final long waitStartMillis = Math.max(tempWaitStartMillis, MINIMUM_DELAY_MILLIS);
        System.out.println("Average compile time millis: " + averageCompileTime + ", delay will be " + waitStartMillis
                + " millis");

        final String className = "TestClass" + startTimeOffset + NUM_COMPILE_TESTS;
        System.out.println(printMillis(System.currentTimeMillis()) + ": starting test with class " + className);
        // We don't want to create the threads until the compile is mostly complete

        final ExecutionContext context = ExecutionContext.getContext();
        for (int i = 0; i < NUM_THREADS; i++) {
            final int fi = i; // For the lambda
            threads[i] = new Thread(() -> {
                context.open();
                try {
                    final long delay = fi == 0 ? 0 : fi * WAIT_BETWEEN_THREAD_START_MILLIS + waitStartMillis;
                    final long startTime = System.currentTimeMillis();
                    compile(fi == 0, className);
                    final long endTime = System.currentTimeMillis();
                    System.out.println(
                            printMillis(endTime) + ": thread " + fi + " completed with specified delay=" + delay
                                    + " (actual run time " + (endTime - startTime) + " millis)");
                } catch (Throwable e) {
                    synchronized (raisedThrowables) {
                        System.out.println("Exception occurred: " + e.getMessage());
                        raisedThrowables.add(e);
                    }
                }
            }, "Compile_" + i);

            threads[i].start();

            if (i == 0) {
                sleepIgnoringInterruptions(waitStartMillis);
            } else {
                sleepIgnoringInterruptions(WAIT_BETWEEN_THREAD_START_MILLIS);
            }
        }

        try {
            for (int i = NUM_THREADS - 1; i >= 0; i--) {
                threads[i].join();
            }
        } catch (InterruptedException ignored) {
        }

        synchronized (raisedThrowables) {
            System.out.println(raisedThrowables.size() +
                    (raisedThrowables.size() == 1 ? " exception was raised" : " exceptions were raised"));
            if (!raisedThrowables.isEmpty()) {
                throw raisedThrowables.get(0);
            }
        }
    }

    private void sleepIgnoringInterruptions(final long waitMillis) {
        try {
            Thread.sleep(waitMillis);
        } catch (InterruptedException ignored) {
        }
    }

    private void compile(boolean printDetails, final String className) {
        final long startMillis;
        if (printDetails) {
            startMillis = System.currentTimeMillis();
            System.out.println(printMillis(startMillis) + ": Thread 0 starting compile");
        } else {
            startMillis = 0;
        }
        ExecutionContext.getContext().getQueryCompiler().compile(
                QueryCompilerRequest.builder()
                        .description("Test Compile")
                        .className(className)
                        .classBody(CLASS_CODE)
                        .packageNameRoot("io.deephaven.temp")
                        .build());
        if (printDetails) {
            final long endMillis = System.currentTimeMillis();
            System.out.println(printMillis(endMillis) + ": Thread 0 ending compile: (" + (endMillis - startMillis)
                    + ") millis elapsed");
        }
    }

    private String printMillis(final long millis) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), DateTimeUtils.timeZone());
        return localDateTime.toString();
    }


    @Test
    public void testSimpleCompile() throws Exception {
        final String program1Text = String.join(
                "\n",
                "public class $CLASSNAME$ {",
                "   public static void main (String [] args) {",
                "      System.out.println (\"Hello, World?\");",
                "      System.out.println (args.length);",
                "   }",
                "   public static class Other {}",
                "}");

        StringBuilder codeLog = new StringBuilder();
        final Class<?> clazz1 = ExecutionContext.getContext().getQueryCompiler().compile(
                QueryCompilerRequest.builder()
                        .description("Test Compile")
                        .className("Test")
                        .classBody(program1Text)
                        .packageNameRoot("com.deephaven.test")
                        .codeLog(codeLog)
                        .build());
        final Method m1 = clazz1.getMethod("main", String[].class);
        Object[] args1 = new Object[] {new String[] {"hello", "there"}};
        m1.invoke(null, args1);
    }

    @Test
    public void testCollidingCompile() throws Exception {
        final String program1Text = String.join(
                "\n",
                "public class Test {",
                "   public static void main (String [] args) {",
                "      System.out.println (\"Hello, World\");",
                "      System.out.println (args.length);",
                "   }",
                "}");

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            Thread t = new Thread(() -> {
                StringBuilder codeLog = new StringBuilder();
                try {
                    final Class<?> clazz1 = ExecutionContext.getContext().getQueryCompiler().compile(
                            QueryCompilerRequest.builder()
                                    .description("Test Compile")
                                    .className("Test")
                                    .classBody(program1Text)
                                    .packageNameRoot("com.deephaven.test")
                                    .codeLog(codeLog)
                                    .build());
                    final Method m1 = clazz1.getMethod("main", String[].class);
                    Object[] args1 = new Object[] {new String[] {"hello", "there"}};
                    m1.invoke(null, args1);
                } catch (Exception e) {
                    throw new UncheckedDeephavenException(e);
                }
            });
            t.start();
            threads.add(t);
        }
        for (final Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void testMultiCompileWithFailure() throws ExecutionException, InterruptedException {
        final String goodProgram = String.join("\n",
                "public class GoodTest {",
                "   public static void main (String [] args) {",
                "   }",
                "}");
        final String badProgram = String.join("\n",
                "public class Formula {",
                "    public Formula() {",
                "        S.badCall(0);",
                "    }",
                "}");

        QueryCompilerRequest[] requests = new QueryCompilerRequest[] {
                QueryCompilerRequest.builder()
                        .description("Test Bad Compile")
                        .className("BadTest")
                        .classBody(badProgram)
                        .packageNameRoot("com.deephaven.test")
                        .build(),
                QueryCompilerRequest.builder()
                        .description("Test Good Compile")
                        .className("GoodTest")
                        .classBody(goodProgram)
                        .packageNameRoot("com.deephaven.test")
                        .build(),
        };

        // noinspection unchecked
        CompletionStageFuture.Resolver<Class<?>>[] resolvers =
                (CompletionStageFuture.Resolver<Class<?>>[]) new CompletionStageFuture.Resolver[] {
                        CompletionStageFuture.make(),
                        CompletionStageFuture.make(),
                };

        ExecutionContext.getContext().getQueryCompiler().compile(requests, resolvers);

        Assert.eqTrue(resolvers[0].getFuture().isDone(), "resolvers[0].getFuture().isDone()");
        Assert.eqTrue(resolvers[1].getFuture().isDone(), "resolvers[0].getFuture().isDone()");
        Assert.neqNull(resolvers[1].getFuture().get(), "resolvers[1].getFuture().get()");
    }

    @Test
    public void testMultiCompileWithFailureSecond() throws ExecutionException, InterruptedException {
        final String badProgram = String.join("\n",
                "public class Formula {",
                "    public Formula() {",
                "        S.badCall(0);",
                "    }",
                "}");
        final String goodProgram = String.join("\n",
                "public class Formula {",
                "   public static void main (String [] args) {",
                "   }",
                "}");

        QueryCompilerRequest[] requests = new QueryCompilerRequest[] {
                QueryCompilerRequest.builder()
                        .description("Test Good Compile")
                        .className("Formula")
                        .classBody(goodProgram)
                        .packageNameRoot("com.deephaven.test")
                        .build(),
                QueryCompilerRequest.builder()
                        .description("Test Bad Compile")
                        .className("Formula")
                        .classBody(badProgram)
                        .packageNameRoot("com.deephaven.test")
                        .build(),
        };

        // noinspection unchecked
        CompletionStageFuture.Resolver<Class<?>>[] resolvers =
                (CompletionStageFuture.Resolver<Class<?>>[]) new CompletionStageFuture.Resolver[] {
                        CompletionStageFuture.make(),
                        CompletionStageFuture.make(),
                };

        ExecutionContext.getContext().getQueryCompiler().compile(requests, resolvers);

        Assert.eqTrue(resolvers[1].getFuture().isDone(), "resolvers[0].getFuture().isDone()");
        Assert.eqTrue(resolvers[0].getFuture().isDone(), "resolvers[0].getFuture().isDone()");
        Assert.neqNull(resolvers[0].getFuture().get(), "resolvers[1].getFuture().get()");
    }
}
