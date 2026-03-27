//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.junit.Assert.*;

public class QueryLibraryTest {
    private SafeCloseable executionContext;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    @After
    public void tearDown() {
        executionContext.close();
    }

    @Test
    public void testImportClass() {
        assertFalse(
                ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                        .contains("import java.util.concurrent.ConcurrentLinkedDeque;"));
        ExecutionContext.getContext().getQueryLibrary().importClass(ConcurrentLinkedDeque.class);
        assertTrue(
                ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                        .contains("import java.util.concurrent.ConcurrentLinkedDeque;"));
    }

    @Test
    public void testPackageClass() {
        assertFalse(ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                .contains("import java.util.concurrent.*;"));
        ExecutionContext.getContext().getQueryLibrary().importPackage(Package.getPackage("java.util.concurrent"));
        assertTrue(ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                .contains("import java.util.concurrent.*;"));
    }

    @Test
    public void testImportStatic() {
        assertFalse(
                ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                        .contains("import static java.util.concurrent.ConcurrentHashMap.*;"));
        ExecutionContext.getContext().getQueryLibrary().importStatic(ConcurrentHashMap.class);
        assertTrue(
                ExecutionContext.getContext().getQueryLibrary().getImportStrings()
                        .contains("import static java.util.concurrent.ConcurrentHashMap.*;"));
    }

    @Test
    public void testImportChangingClass() throws ClassNotFoundException {
        // Keep track of the initial classloader to avoid affecting other tests
        ClassLoader original = Thread.currentThread().getContextClassLoader();

        try {
            // Define a classloader for our new class and load it
            String binaryName = "com.test.Foo";
            ClassLoader cl = makeClassloaderWithClass(binaryName, 123);
            Class<?> aClass = cl.loadClass(binaryName);

            QueryLibrary ql = ExecutionContext.getContext().getQueryLibrary();
            ql.importClass(aClass);

            // Validate that the class doesn't appear in imports if we try the default classloader
            assertFalse(ql.getImportStrings()
                    .contains("import " + binaryName + ";"));

            // Set up the context classloader to be our new classloader, and validate that we can now see the class
            Thread.currentThread().setContextClassLoader(cl);
            assertTrue(ql.getImportStrings()
                    .contains("import " + binaryName + ";"));

            // Make a new classloader with the same class name, but a different class definition
            ClassLoader cl2 = makeClassloaderWithClass(binaryName, 456);
            Class<?> aClass2 = cl2.loadClass(binaryName);
            assertNotEquals(aClass, aClass2);
            Thread.currentThread().setContextClassLoader(cl2);

            assertTrue(ql.getImportStrings()
                    .contains("import " + binaryName + ";"));
            assertFalse(ql.getClassImports().contains(aClass));
            assertTrue(ql.getClassImports().contains(aClass2));
        } finally {
            // Restore the old classloader
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    /**
     * Helper to generate a classloader with a single class in it. That class will have a single field, called "count",
     * with the value provided as an argument.
     */
    private static ClassLoader makeClassloaderWithClass(String binaryName, int value) {
        String internalName = binaryName.replace('.', '/');
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC, internalName, null, "java/lang/Object", null);

        cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "count", "I", null, value)
                .visitEnd();

        cw.visitEnd();
        byte[] bytes = cw.toByteArray();
        return new ClassLoader() {
            {
                defineClass(binaryName, bytes, 0, bytes.length);
            }
        };
    }
}
