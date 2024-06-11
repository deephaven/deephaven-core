//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.scripts;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.GroovyDeephavenSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.function.Numeric;
import io.deephaven.function.Sort;
import io.deephaven.plugin.type.ObjectTypeLookup.NoOp;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestGroovyDeephavenSession {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private LivenessScope livenessScope;
    private GroovyDeephavenSession session;
    private SafeCloseable executionContext;

    @Before
    public void setup() throws IOException {
        livenessScope = new LivenessScope();
        LivenessScopeStack.push(livenessScope);
        final ExecutionContext context = ExecutionContext.getContext();
        session = GroovyDeephavenSession.of(
                context.getUpdateGraph(), context.getOperationInitializer(), NoOp.INSTANCE,
                GroovyDeephavenSession.RunScripts.none());
        executionContext = session.getExecutionContext().open();
    }

    @After
    public void teardown() {
        executionContext.close();
        LivenessScopeStack.pop(livenessScope);
        livenessScope.release();
        livenessScope = null;
    }

    public <T> T fetch(final String name, final Class<T> clazz) {
        // note var is guaranteed to be non-null
        final Object var = session.getQueryScope().readParamValue(name);
        if (clazz.isAssignableFrom(var.getClass())) {
            // noinspection unchecked
            return (T) var;
        }
        throw new RuntimeException("Unexpected type for variable '" + name + "'. Found: "
                + var.getClass().getCanonicalName() + " Expected: " + clazz.getCanonicalName());
    }

    public Table fetchTable(final String name) {
        return fetch(name, Table.class);
    }

    @Test
    public void testNullCast() {
        session.evaluateScript("x = null; y = emptyTable(0).update(\"X = (java.util.List)x\")").throwIfError();
        final Table y = fetchTable("y");
        final TableDefinition definition = y.getDefinition();
        final Class<?> colClass = definition.getColumn("X").getDataType();
        Assert.assertEquals(colClass, java.util.List.class);
    }

    @Test
    public void testScriptDefinedClass() {
        session.evaluateScript("class MyObj {\n" +
                "    public int a;\n" +
                "    MyObj(int a) {\n" +
                "        this.a = a\n" +
                "    }\n" +
                "}\n" +
                "obj = new MyObj(1)\n" +
                "result = emptyTable(1).select(\"A = obj.a\")").throwIfError();
        Assert.assertNotNull(fetch("obj", Object.class));
        final Table result = fetchTable("result");
        Assert.assertFalse(result.isFailed());
    }

    @Test
    public void testImportedScriptDefinedClass() {
        session.evaluateScript("import io.deephaven.engine.util.scripts.MainScript").throwIfError();
        session.evaluateScript("t = MainScript.makeTheData(100).update('X = Data.getX()', 'Y = Data.getY()')")
                .throwIfError();
        Table result = fetchTable("t");
        assertEquals(100, result.size());
        assertEquals(3, result.numColumns());
        assertEquals(double.class, result.getColumnSource("X").getType());
        assertEquals(double.class, result.getColumnSource("Y").getType());
    }

    @Test
    public void testScriptResultOrder() {
        final ScriptSession.Changes changes = session.evaluateScript("x=emptyTable(10)\n" +
                "z=emptyTable(10)\n" +
                "y=emptyTable(10)\n" +
                "u=emptyTable(10)");
        changes.throwIfError();
        final String[] names = new String[] {"x", "z", "y", "u"};
        final MutableInt offset = new MutableInt();
        changes.created.forEach((name, type) -> {
            Assert.assertEquals(name, names[offset.getAndIncrement()]);
        });
    }

    @Test
    public void testUnloadedWildcardPackageImport() {
        // Pick three packages we won't have loaded directly - a JRE package, a third party package (from a jar), and a
        // package in the current source set
        for (String packageString : Set.of("java.util", "groovy.time", "io.deephaven.engine.util.scripts.wontbeused")) {
            if (this.getClass().getClassLoader().getDefinedPackage(packageString) != null) {
                Assert.fail("Package '" + packageString + "' is already loaded, test with a more obscure package.");
            }
            session.evaluateScript("import " + packageString + ".*").throwIfError();
        }
    }

    /**
     * This is a value used in testGroovyClassReload for a formula.
     */
    public static int VALUE_FOR_IMPORT = 45;

    @Test
    public void testGroovyClassReload() throws IOException {
        session.evaluateScript(
                "public class MyClass1 { public static int getMyVar() { return 42 ; } };\n ExecutionContext.getContext().getQueryLibrary().importClass(MyClass1);\nt = emptyTable(1).update(\"Var=MyClass1.getMyVar()\");\n",
                "Script1").throwIfError();
        final Table t = session.getQueryScope().readParamValue("t");
        final int var1 = t.getColumnSource("Var").getInt(0);
        assertEquals(42, var1);

        session.evaluateScript(
                "public class MyClass1 { public static int getMyVar() { return 43 ; } };\n ExecutionContext.getContext().getQueryLibrary().importClass(MyClass1);\n t2 = emptyTable(1).update(\"Var=MyClass1.getMyVar()\");\n",
                "Script2").throwIfError();
        final Table t2 = session.getQueryScope().readParamValue("t2");
        final int var2 = t2.getColumnSource("Var").getInt(0);
        assertEquals(43, var2);

        // we are not actually importing a Groovy class or reloading here, but it is convenient place to put the
        // importClass call of a regular class, so that we can examine what happens in a debugger and
        // learn how to differentiate it from a groovy class.
        session.evaluateScript("ExecutionContext.getContext().getQueryLibrary().importClass("
                + getClass().getCanonicalName() + ".class);\n t3 = emptyTable(1).update(\"Var="
                + getClass().getCanonicalName() + ".VALUE_FOR_IMPORT\");\n", "Script3").throwIfError();
        final Table t3 = session.getQueryScope().readParamValue("t3");
        final int var3 = t3.getColumnSource("Var").getInt(0);
        assertEquals(VALUE_FOR_IMPORT, var3);
    }

    /**
     * test the static removeComments method in GroovyDeephavenSession.
     */
    @Test
    public void testRemoveComments() {
        // pre and post strings separated by pipe character
        final String[] testCases = new String[] {
                "abc|abc",
                "/* x */ foo //x|foo",
                "// /* whatever */ |",
                "/*foo//bar */|",
                "foo/*|foo/*",
                "foo/*/bar|foo/*/bar",
                "foo/**/bar|foobar",
                "/* x /* y */ z */|z */",
                "abc /* these comments take precedence over // comments */ abc|abc  abc",
                "abc\ndef/* this should be ignored\n   across multiple lines\n */// also ignored\nghi|abc\ndef\nghi",
                // escaping - ignore for now
                // quoting - ignore for now, they are not valid in the import statement context
        };
        for (String testCase : testCases) {
            final String[] parts = testCase.split("\\|");
            final String p1 = parts.length > 1 ? parts[1] : "";
            assertEquals(p1, GroovyDeephavenSession.removeComments(parts[0]));
            // System.out.println("Pass: \"" + parts[0] + "\" -> \"" + p1 + "\"");
        }
    }

    // things to import for testIsValidImportString
    @SuppressWarnings("unused")
    public static class StaticClass {
        public static int field;
        public static int field1;
        public static int field2;

        public static void method() {}

        public static void method1() {}

        public static void method2() {}

        public static class InnerStaticClass {
            public static int field;
            public static int field1;
            public static int field2;

            public static void method() {}

            public static void method1() {}

            public static void method2() {}

            public static class InnerInnerStaticClass {
                public static int field;
                public static int field1;
                public static int field2;

                public static void method() {}

                public static void method1() {}

                public static void method2() {}
            }
        }
    }

    @SuppressWarnings("unused")
    // testIsValidImportString
    public int field;

    @SuppressWarnings("unused")
    // for testIsValidImportString
    public void method() {}


    /**
     * test the code that checks an import request for validity.
     */
    @Test
    public void testIsValidImportString() throws IOException {
        final String[] failTestCases = new String[] {
                // two imports
                "import import io.deephaven.engine.util.TableTools",
                // static before import
                "static import io.deephaven.engine.util.TableTools",
                // invalid after stripped comments
                "import // io.deephaven.engine.util.TableTools",
                // trailing .
                "import io.deephaven.engine.util.TableTools.",
                // invalid wildcard
                "import io.deephaven.engine.util.TableTools.**",
                // doubled wildcard
                "import io.deephaven.engine.util.TableTools.*.*",
                // internal space
                "import io.deephaven.engine. util.TableTools; // has a space",
                // nonexistent package
                "import io.deephaven.engine.xxx.util.TableTools",
                // nonexistent classname
                "import io.deephaven.engine.util.TablexxxTools",
                // includes "static" but is (correctly) not stripped out
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClassstatic ",
                // import and static correctly not removed at the end
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass import static",
                // semicolons correctly not removed internally
                "import static io.deephaven.engine.util.scripts.Test;Groovy;DeephavenSession.StaticClass",
                // not valid non-static imports of fields and methods
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.field  ;",
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.method ; ; ;",
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.field",
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.method",
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.InnerInnerStaticClass.field",
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.InnerInnerStaticClass.method",
                // missing spaces
                "importstaticio.deephaven.engine.util.scripts.TestGroovyDeephavenSession;",
                // missing spaces
                "importstatic io.deephaven.engine.util.scripts.TestGroovyDeephavenSession;",
                // missing spaces
                "import staticio.deephaven.engine.util.scripts.TestGroovyDeephavenSession;",
                // import is not optional, ; is optional;
                "static    io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass",
                // groovy is not cool with non-star package imports for packages that don't exist
                "import com.illumon.foo.bar;",
                // make sure illegal java identifiers don't get through
                "import com.123.foo.bar.*;",
                "import com.1illumon.*;",
                "import com.ill-umon.*;",
                // valid imports that can't be aliased
                "import static io.deephaven.engine.util.scripts.* as wild",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.* as wild",
        };
        final String[] succeedTestCases = new String[] {
                // ; is optional
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass",
                // ; is optional
                "import   static    io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass",
                // /* comment */ removed
                "import   static /* static */   io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass;",
                // //comment removed
                " import    io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass // whatever .*;",

                // static imports of class, field, method, wildcards - all several levels deep
                // no semicolon
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession",
                // semicolon
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.*;",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.field  ;",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.method ; ; ;",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.*",
                // no semicolon
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass",
                // semicolon
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.*;",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.field  ;",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.method ; ; ;",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.*",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.field",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.method",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.InnerInnerStaticClass",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.InnerInnerStaticClass.*",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.InnerInnerStaticClass.field",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.InnerInnerStaticClass.method",

                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass;",
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.*;",
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass",
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.*",
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.InnerInnerStaticClass",
                "import io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.InnerStaticClass.InnerInnerStaticClass.*",

                // import package
                "import io.deephaven.engine.util.*",
                // the following succeed, but java does not like them
                "import java./*foo */lang.reflect.Method;",

                // groovy is cool with package star imports for packages that don't exist
                // ... but we are not enabling that check
                // "import io.deephaven.foo.bar.*",

                // Verify non-wildcard cases with "as" to alias the import to something else
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession as TestType",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession as TestType ; ",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.field as fieldName",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.field as fieldName;",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.method as methodName",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.method as methodName ; ; ;",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass as StaticType",
                "import static io.deephaven.engine.util.scripts.TestGroovyDeephavenSession.StaticClass.field as fieldName",
        };

        // Note that in `GroovyDeephavenSession`, the _entire_ inbound-command is stripped of comments prior to
        // individual lines being passed to `isValidImportString()`. to replicate that behavior, we must
        // `removeComments()` from each string we are testing here.
        for (String testCase : failTestCases) {
            Optional<GroovyDeephavenSession.GroovyImport> result =
                    session.createImport(GroovyDeephavenSession.removeComments(testCase));
            assertNull("expected failure for: " + testCase, result.orElse(null));
        }

        for (String testCase : succeedTestCases) {
            Optional<GroovyDeephavenSession.GroovyImport> result =
                    session.createImport(GroovyDeephavenSession.removeComments(testCase));

            assertNotNull("expected success for: " + testCase, result.orElse(null));
        }

        // special package import cases
        // this package is unknown
        // DHC change in behavior: as a fix for #1129, we now use classgraph to find packages with no loaded classes
        assertNotNull("expect failure for unknown package: com.google.common.html",
                session.createImport(GroovyDeephavenSession.removeComments("import com.google.common.html.*;")));
        // Now load the class
        assertNotNull("expect to find class: com.google.common.html.HtmlEscapers", session.createImport(
                GroovyDeephavenSession.removeComments("import com.google.common.html.HtmlEscapers;")));
        // confirm non-classgraph package finding works
        assertNotNull("expect to find package: com.google.common.html",
                session.createImport(GroovyDeephavenSession.removeComments("import com.google.common.html.*;")));

    }

    @Test
    public void testRewriteStackTrace() {
        try {
            session.evaluateScript("println \"Hello\";\nthrow new RuntimeException(\"Bad Line\");\nprintln \"Bye\";\n",
                    "Script2").throwIfError();
            fail("failed to error out");
        } catch (RuntimeException e) {
            assertEquals("Error encountered at line 2: throw new RuntimeException(\"Bad Line\");", e.getMessage());
            final Throwable cause = e.getCause();
            assertTrue(cause instanceof RuntimeException);
            // noinspection ConstantConditions
            if (cause != null) {
                assertEquals("Bad Line", cause.getMessage());
            }
        }

        try {
            session.evaluateScript(
                    "println \"Going to source\";\nimport io.deephaven.engine.util.scripts.Imported;\nImported.main()\n",
                    "Script3").throwIfError();
            fail("failed to error out");
        } catch (RuntimeException e) {
            assertEquals("Error encountered at line 3: Imported.main()", e.getMessage());
            final Throwable cause = e.getCause();
            assertEquals(RuntimeException.class, cause.getClass());
            assertEquals("Busted", cause.getMessage());
            assertNull(cause.getCause());
        }
        try {
            session.evaluateScript("println(Imported.main())").throwIfError();
            fail("failed to error out");
        } catch (Exception e) {
            assertEquals("Busted", e.getMessage());
            assertNull(e.getCause());
        }

        try {
            session.evaluateScript(
                    "println \"Hello there\";\n\n// some more blank lines\n\n\n\n\n\n\n\nprintln \"Going to source\";\nimport io.deephaven.engine.util.scripts.ShortSource;\nShortSource.main();\n",
                    "Script4").throwIfError();
            fail("failed to error out");
        } catch (RuntimeException e) {
            // Check for the re-thrown exception pointing at our evaluated groovy command
            assertEquals("Error encountered at line 13: ShortSource.main();", e.getMessage());

            // Confirm that it wraps the thrown RuntimeException in the imported script
            final Throwable cause = e.getCause();
            assertEquals(RuntimeException.class, cause.getClass());
            assertEquals("Busted Short", cause.getMessage());
            assertNull(cause.getCause());
        }
    }

    @Test
    public void testPotentialAmbiguousMethodCalls() {
        int[] a = new int[] {5, 2, 3};
        int z = 1;
        int Y = 2;
        double d = 5d;
        String c = null;
        try {
            c = "primMin = min(" + Arrays.toString(a).substring(1, Arrays.toString(a).length() - 1) + ");\n";
            session.evaluateScript(c).throwIfError();
            Integer primMin = session.getQueryScope().readParamValue("primMin");
            assertEquals(Numeric.min(a), primMin.intValue());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }

        try {

            c = "z = " + z + "; \n" + "d = " + d + "; \n" +
                    "wrapMin = min(" + Arrays.toString(a).substring(1, Arrays.toString(a).length() - 1) + ", z);\n";
            session.evaluateScript(c).throwIfError();
            Integer wrapperMin = session.getQueryScope().readParamValue("wrapMin");
            assertEquals(Math.min(Numeric.min(a), z), wrapperMin.intValue());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }

        try {
            c = "z = " + z + "; \n" + "d = " + d + "; \n" +
                    "m2 = max(" + Arrays.toString(a).substring(1, Arrays.toString(a).length() - 1) + ", z, d);\n";
            session.evaluateScript(c).throwIfError();
            Double wrapperMax = session.getQueryScope().readParamValue("m2");
            assertEquals(5.0d, wrapperMax, 0.0d);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }

        c = "t = emptyTable(1).updateView(\"Y=" + Y + "\", \"Z=min(Y, z)\")\n";
        try {
            QueryScope.addParam("z", z);
            session.evaluateScript(c).throwIfError();
            final Table t = session.getQueryScope().readParamValue("t");
            final int var2 = t.getColumnSource("Z").getInt(0);
            assertEquals(Numeric.min(Y, z), var2);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }

        c = "t = emptyTable(1).updateView(\"Y=" + Y + "\", \"Z=min(Y, 5d)\")\n";
        try {
            QueryScope.addParam("z", z);
            session.evaluateScript(c).throwIfError();
            final Table t = session.getQueryScope().readParamValue("t");
            final double var2 = t.getColumnSource("Z").getDouble(0);
            assertEquals(Numeric.min(Y, 5d), var2, 1e-10);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }

        c = "t = emptyTable(1).updateView(\"Y=" + Y + "\", \"Z=min(Y, d)\")\n";
        try {
            QueryScope.addParam("z", z);
            QueryScope.addParam("d", d);
            session.evaluateScript(c).throwIfError();
            final Table t = session.getQueryScope().readParamValue("t");
            final double var2 = t.getColumnSource("Z").getDouble(0);
            assertEquals(Numeric.min(Y, d), var2, 1e-10);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }

        c = "t = emptyTable(1).updateView(\"Y=" + Y + "\", \"Z=max(Y, z)\")\n";
        try {
            QueryScope.addParam("z", z);
            session.evaluateScript(c).throwIfError();
            final Table t = session.getQueryScope().readParamValue("t");
            final int var2 = t.getColumnSource("Z").getInt(0);
            assertEquals(Numeric.max(Y, z), var2);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }


        c = "t = emptyTable(1).updateView(\"Y=" + Y + "\", \"Z=max(Y, 5d)\")\n";
        try {
            QueryScope.addParam("z", z);
            session.evaluateScript(c).throwIfError();
            final Table t = session.getQueryScope().readParamValue("t");
            final double var2 = t.getColumnSource("Z").getDouble(0);
            assertEquals(Numeric.max(Y, 5d), var2, 1e-10);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }

        c = "t = emptyTable(1).updateView(\"Y=" + Y + "\", \"Z=max(Y, d)\")\n";
        try {
            QueryScope.addParam("z", z);
            QueryScope.addParam("d", d);
            session.evaluateScript(c).throwIfError();
            final Table t = session.getQueryScope().readParamValue("t");
            final double var2 = t.getColumnSource("Z").getDouble(0);
            assertEquals(Numeric.max(Y, d), var2, 1e-10);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }

        c = "t = emptyTable(1).updateView(\"Y=" + Y + "\", \"Z=sort(Y, z)\")\n";
        try {
            QueryScope.addParam("z", z);
            session.evaluateScript(c).throwIfError();
            final Table t = session.getQueryScope().readParamValue("t");
            final int[] var2 = t.getColumnSource("Z", int[].class).get(0);
            assertArrayEquals(Sort.sort(Y, z), var2);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }

        // TODO (deephaven-core#4570) sortObj fails with mixed number types
        // c = "t = emptyTable(1).updateView(\"Y=" + Y
        // + "\", \"Z=io.deephaven.function.Sort.<Comparable>sortObj(new Comparable[]{Y, d})\")\n";
        // try {
        // QueryScope.addParam("z", z);
        // QueryScope.addParam("d", d);
        // session.evaluateScript(c).throwIfError();
        // final Table t = session.getQueryScope().readParamValue("t");
        // final Comparable[] var2 = t.getColumnSource("Z", Comparable[].class).get(0);
        // // noinspection unchecked
        // assertArrayEquals(Sort.<Comparable>sortObj(Y, d), var2);
        // } catch (Exception e) {
        // e.printStackTrace();
        // fail("Fail for : \n" + c);
        // }

        c = "t = emptyTable(1).updateView(\"Y=" + Y + "\", \"Z=sortDescending(Y, z)\")\n";
        try {
            QueryScope.addParam("z", z);
            session.evaluateScript(c).throwIfError();
            final Table t = session.getQueryScope().readParamValue("t");
            final int[] var2 = t.getColumnSource("Z", int[].class).get(0);
            assertArrayEquals(Sort.sortDescending(Y, z), var2);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }

        // TODO (deephaven-core#4570) sortObj fails with mixed number types
        // c = "t = emptyTable(1).updateView(\"Y=" + Y + "\", \"Z=sortDescending(new Number[]{Y, d})\")\n";
        // try {
        // QueryScope.addParam("z", z);
        // QueryScope.addParam("d", d);
        // session.evaluateScript(c).throwIfError();
        // final Table t = (Table) session.getVariable("t");
        // final Number[] var2 = t.getColumnSource("Z", Number[].class).get(0);
        // // noinspection unchecked
        // assertArrayEquals(Sort.<Comparable>sortDescendingObj(Y, d), var2);
        // } catch (Exception e) {
        // e.printStackTrace();
        // fail("Fail for : \n" + c);
        // }

        c = "t = emptyTable(1).updateView(\"Y=" + Y + "\", \"Z=ssVec(Y, z)\")\n";
        try {
            QueryScope.addParam("z", z);
            session.evaluateScript(c).throwIfError();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Fail for : \n" + c);
        }
    }

    @Test
    public void testMinInFormula() {
        QueryScope.addParam("d", 5d);
        session.evaluateScript("t = emptyTable(1).updateView(\"Y=1\", \"Z=min(Y,d)\")\n").throwIfError();
        final Table t = session.getQueryScope().readParamValue("t");
    }
}

