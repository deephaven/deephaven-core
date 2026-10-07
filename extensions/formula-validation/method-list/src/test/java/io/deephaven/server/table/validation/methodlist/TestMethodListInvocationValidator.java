//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.server.table.validation.methodlist.other.OtherPackageNonWideningSub;
import io.deephaven.server.table.validation.methodlist.other.OtherPackageProtectedSub;
import io.deephaven.server.table.validation.methodlist.other.OtherPackageSub;
import io.deephaven.server.table.validation.methodlist.other.OtherPackageTransitiveSub;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestMethodListInvocationValidator {
    @Test
    public void testDeclaringClass() throws NoSuchMethodException {
        assertPermitted("java.lang.Math abs(..)", Math.class.getMethod("abs", double.class));
        assertPermitted("java.time.Instant *(..)", Instant.class.getMethod("parse", CharSequence.class));
        assertPermitted("java.lang.Integer *(int)", Integer.class.getMethod("valueOf", int.class));
        assertNotPermitted("java.lang.Integer *(int)", Integer.class.getMethod("valueOf", String.class));
        assertNotPermitted("java.lang.Long *(..)", Integer.class.getMethod("valueOf", int.class));
        assertPermitted("java.lang.String to*Case()", String.class.getMethod("toUpperCase"));
        // a class pattern does not match a method the class inherits without overriding
        assertNotPermitted("java.lang.String *(..)", String.class.getMethod("getClass"));
        assertNotPermitted("java.lang.String to*Case()", String.class.getMethod("toString"));
        assertPermitted("java.lang.Ma* max(..)", Math.class.getMethod("max", int.class, int.class));
    }

    @Test
    public void testPackageWildcards() throws NoSuchMethodException {
        assertPermitted("*..* *(..)", Math.class.getMethod("abs", int.class));
        assertPermitted("java.lang.* *(..)", Math.class.getMethod("max", int.class, int.class));
        assertPermitted("java..* *(..)", Math.class.getMethod("max", int.class, int.class));
        assertPermitted("java.util.* *(..)", ArrayList.class.getMethod("trimToSize"));
        // "*" does not cross a package boundary, ".." does
        assertNotPermitted("java.util.* *(..)", ConcurrentHashMap.class.getMethod("mappingCount"));
        assertPermitted("java.util..* *(..)", ConcurrentHashMap.class.getMethod("mappingCount"));
        assertNotPermitted("javax..* *(..)", ConcurrentHashMap.class.getMethod("mappingCount"));
    }

    @Test
    public void testNestedClasses() throws NoSuchMethodException {
        assertPermitted("java.util.Map.Entry getKey()", Map.Entry.class.getMethod("getKey"));
        assertPermitted("java.util.Map$Entry getKey()", Map.Entry.class.getMethod("getKey"));
        assertNotPermitted("java.util.Entry getKey()", Map.Entry.class.getMethod("getKey"));
        // "*" matches the binary name of a nested class, and ".." crosses enclosing class names
        assertPermitted("java.util.* getKey()", Map.Entry.class.getMethod("getKey"));
        assertPermitted("java.util..Entry getKey()", Map.Entry.class.getMethod("getKey"));
        assertNotPermitted("java.util.*.Entry getKey()", ConcurrentHashMap.class.getMethod("mappingCount"));
    }

    @Test
    public void testArguments() throws NoSuchMethodException {
        assertPermitted("java.lang.String valueOf(char[])", String.class.getMethod("valueOf", char[].class));
        assertNotPermitted("java.lang.String valueOf(java.lang.Object[])",
                String.class.getMethod("valueOf", char[].class));
        assertPermitted("java.lang.String valueOf(*)", String.class.getMethod("valueOf", int.class));
        assertPermitted("java.lang.String valueOf(*)", String.class.getMethod("valueOf", Object.class));
        assertPermitted("java.lang.String valueOf(*)", String.class.getMethod("valueOf", char[].class));
        assertPermitted("java.lang.Math max(*, *)", Math.class.getMethod("max", int.class, int.class));
        assertNotPermitted("java.lang.Math max(*)", Math.class.getMethod("max", int.class, int.class));
        assertPermitted("java.lang.Math max(int, int)", Math.class.getMethod("max", int.class, int.class));
        assertNotPermitted("java.lang.Math max(long, long)", Math.class.getMethod("max", int.class, int.class));
        assertPermitted("java.lang.String *(int, ..)", String.class.getMethod("substring", int.class, int.class));
        assertPermitted("java.lang.String *(int, ..)", String.class.getMethod("substring", int.class));
        assertNotPermitted("java.lang.String *(int, ..)", String.class.getMethod("indexOf", String.class, int.class));
        assertPermitted("java.lang.String *(.., int)", String.class.getMethod("indexOf", String.class, int.class));
        assertNotPermitted("java.lang.String *(.., int)", String.class.getMethod("indexOf", String.class));
        assertNotPermitted("java.lang.String *(.., int)", String.class.getMethod("length"));
        assertPermitted("java.util.Arrays toString(int[])", Arrays.class.getMethod("toString", int[].class));
        assertNotPermitted("java.util.Arrays toString(int[])", Arrays.class.getMethod("toString", long[].class));
        // each [] after a wildcard matches exactly one array dimension
        final String arrays = ArrayParameters.class.getName();
        final Method vector = ArrayParameters.class.getMethod("vector", String[].class);
        final Method matrix = ArrayParameters.class.getMethod("matrix", String[][].class);
        final Method primitiveMatrix = ArrayParameters.class.getMethod("primitiveMatrix", int[][].class);
        for (final String wildcard : List.of("*", "*..*")) {
            assertPermitted(arrays + " *(" + wildcard + "[])", vector);
            assertNotPermitted(arrays + " *(" + wildcard + "[])", matrix);
            assertNotPermitted(arrays + " *(" + wildcard + "...)", matrix);
            assertPermitted(arrays + " *(" + wildcard + "[][])", matrix);
            assertPermitted(arrays + " *(" + wildcard + "...)", vector);
            assertNotPermitted(arrays + " *(" + wildcard + "[][])", vector);
            assertPermitted(arrays + " *(" + wildcard + "[][])", primitiveMatrix);
        }
        // while a wildcard alone matches any parameter type, arrays included
        assertPermitted(arrays + " *(*)", matrix);
        // an array parameter may be in any position
        assertPermitted("java.util.Arrays fill(int[], int)", Arrays.class.getMethod("fill", int[].class, int.class));
        assertPermitted("java.util.Arrays deepToString(java.lang.Object[])",
                Arrays.class.getMethod("deepToString", Object[].class));
        // a wildcard within a name does not match an array type
        final Method join = String.class.getMethod("join", CharSequence.class, CharSequence[].class);
        assertNotPermitted("java.lang.String join(java.lang.*, java.lang.*)", join);
        assertPermitted("java.lang.String join(java.lang.*, java.lang.*[])", join);
        assertPermitted("java.lang.String join(java.lang.*, *)", join);
        final Method deepToString = Arrays.class.getMethod("deepToString", Object[].class);
        assertNotPermitted("java.util.Arrays deepToString(java.lang.*)", deepToString);
        assertPermitted("java.util.Arrays deepToString(java.lang.*[])", deepToString);
    }

    @Test
    public void testVarargs() throws NoSuchMethodException {
        final Method format = String.class.getMethod("format", String.class, Object[].class);
        assertPermitted("java.lang.String format(java.lang.String, java.lang.Object[])", format);
        assertPermitted("java.lang.String format(java.lang.String, java.lang.Object...)", format);
        assertPermitted("java.util.Arrays asList(Object[])", Arrays.class.getMethod("asList", Object[].class));
        assertPermitted("java.util.Arrays asList(Object...)", Arrays.class.getMethod("asList", Object[].class));

        // the variable arity parameter must still match its element type and position
        assertNotPermitted("java.lang.String format(java.lang.String, java.lang.String...)", format);
        assertNotPermitted("java.lang.String format(java.lang.String, int...)", format);
        assertNotPermitted("java.lang.String format(java.lang.Object...)", format);
        assertNotPermitted("java.lang.String format(java.lang.String...)", format);
        assertNotPermitted("java.util.Arrays asList(int...)", Arrays.class.getMethod("asList", Object[].class));
        // a variable arity pattern matches only an array parameter, not the element type or a missing parameter
        assertNotPermitted("java.lang.String format(java.lang.String, java.lang.Object)", format);
        assertNotPermitted("java.lang.String format(java.lang.String)", format);
        assertNotPermitted("java.lang.String valueOf(java.lang.Object...)",
                String.class.getMethod("valueOf", Object.class));
        assertNotPermitted("java.lang.String join(java.lang.CharSequence, java.lang.CharSequence...)",
                String.class.getMethod("join", CharSequence.class, Iterable.class));
        assertPermitted("java.lang.String join(java.lang.CharSequence, java.lang.CharSequence...)",
                String.class.getMethod("join", CharSequence.class, CharSequence[].class));
    }

    @Test
    public void testUnqualifiedTypes() throws NoSuchMethodException {
        // only java.lang types may be unqualified
        assertPermitted("java.lang.String valueOf(Object)", String.class.getMethod("valueOf", Object.class));
        assertPermitted("String length()", String.class.getMethod("length"));
        assertNotPermitted("Collections emptyMap()", Collections.class.getMethod("emptyMap"));
        // an unqualified name with a wildcard is not taken to be in java.lang, so it matches only the unnamed package
        assertNotPermitted("java.lang.String valueOf(Obj*)", String.class.getMethod("valueOf", Object.class));
        assertNotPermitted("Str* length()", String.class.getMethod("length"));
        assertNotPermitted("java.util.Collections unmodifiableMap(Map)",
                Collections.class.getMethod("unmodifiableMap", Map.class));
        assertPermitted("java.util.Collections unmodifiableMap(java.util.Map)",
                Collections.class.getMethod("unmodifiableMap", Map.class));
    }

    @Test
    public void testMissingTypes() throws NoSuchMethodException {
        assertNotPermitted("com.example.Missing length()", String.class.getMethod("length"));
        assertNotPermitted("java.lang.String *(com.example.Missing)", String.class.getMethod("length"));
        assertNotPermitted("java.lang.String *(com.example.Missing)",
                String.class.getMethod("valueOf", Object.class));
    }

    @Test
    public void testObjectOverrides() throws NoSuchMethodException {
        assertPermitted("java.lang.Object toString()", Object.class.getMethod("toString"));
        assertPermitted("java.lang.Object toString()", Integer.class.getMethod("toString"));
        assertPermitted("java.lang.Object toString()", StringBuilder.class.getMethod("toString"));
        assertPermitted("java.lang.Object hashCode()", Integer.class.getMethod("hashCode"));
        assertPermitted("java.lang.Object equals(java.lang.Object)", String.class.getMethod("equals", Object.class));
        assertPermitted("java.lang.Object getClass()", Integer.class.getMethod("getClass"));
        // an interface that declares a method of Object overrides it, though Object is not among its generic supertypes
        final Method comparatorEquals = java.util.Comparator.class.getMethod("equals", Object.class);
        Assert.assertEquals(java.util.Comparator.class, comparatorEquals.getDeclaringClass());
        assertPermitted("java.lang.Object equals(java.lang.Object)", comparatorEquals);
        final Method charSequenceToString = CharSequence.class.getMethod("toString");
        Assert.assertEquals(CharSequence.class, charSequenceToString.getDeclaringClass());
        assertPermitted("java.lang.Object toString()", charSequenceToString);
        // but it declares only the public methods of Object, so it does not override a protected one
        final Method interfaceClone = CloneableInterface.class.getMethod("clone");
        assertNotPermitted("java.lang.Object clone()", interfaceClone);
        assertPermitted(CloneableInterface.class.getName() + " clone()", interfaceClone);
        // static methods do not override
        assertNotPermitted("java.lang.Object hashCode()", Integer.class.getMethod("hashCode", int.class));
        assertNotPermitted("java.lang.Object toString(..)", Integer.class.getMethod("toString", int.class));
        assertNotPermitted("java.lang.Object *(..)", Integer.class.getMethod("valueOf", int.class));
        // an override is matched by its supertype, not the other way around
        assertNotPermitted("java.lang.String toString()", Object.class.getMethod("toString"));
    }

    @Test
    public void testOverrides() throws NoSuchMethodException {
        assertPermitted("java.lang.Number *(..)", Number.class.getMethod("intValue"));
        assertPermitted("java.lang.Number *(..)", Integer.class.getMethod("intValue"));
        assertPermitted("java.lang.Number intValue()", BigDecimal.class.getMethod("intValue"));
        assertNotPermitted("java.lang.Number *(..)", Integer.class.getMethod("valueOf", int.class));
        assertNotPermitted("java.lang.Number *(..)", Integer.class.getMethod("compareTo", Integer.class));
        assertPermitted("java.lang.CharSequence length()", String.class.getMethod("length"));
        assertPermitted("java.util.List size()", ArrayList.class.getMethod("size"));
        assertPermitted("java.util.Collection size()", ArrayList.class.getMethod("size"));
        assertNotPermitted("java.util.List size()", ConcurrentHashMap.class.getMethod("size"));
        assertNotPermitted("java.util.List trimToSize()", ArrayList.class.getMethod("trimToSize"));
        // a covariant return type still overrides
        assertPermitted("java.lang.Appendable append(java.lang.CharSequence)",
                StringBuilder.class.getMethod("append", CharSequence.class));
    }

    @Test
    public void testNonFinalDefaultClasses() throws NoSuchMethodException {
        // the default allowlist names non-final classes, whose patterns also match overrides in subclasses
        final String pattern = "java.math.BigInteger *(..)";
        assertPermitted(pattern, CustomBigInteger.class.getMethod("add", BigInteger.class));
        assertPermitted(pattern, CustomBigInteger.class.getMethod("toString"));
        // but not the subclass's other methods, nor its constructors
        assertNotPermitted(pattern, CustomBigInteger.class.getMethod("extra"));
        assertNotPermitted(pattern, CustomBigInteger.class.getConstructor());
        assertPermitted("java.math.BigDecimal *(..)", CustomBigDecimal.class.getMethod("scale"));
        assertNotPermitted("java.math.BigDecimal *(..)", CustomBigDecimal.class.getMethod("extra"));
    }

    @Test
    public void testGenericOverrides() throws NoSuchMethodException {
        final Method compareTo = Integer.class.getMethod("compareTo", Integer.class);
        assertPermitted("java.lang.Comparable compareTo(..)", compareTo);
        assertPermitted("java.lang.Comparable compareTo(java.lang.Object)", compareTo);
        assertPermitted("java.lang.Comparable compareTo(java.lang.Integer)", compareTo);
        assertPermitted("java.lang.Comparable compareTo(*)", compareTo);
        assertNotPermitted("java.lang.Comparable compareTo(java.lang.String)", compareTo);
        assertNotPermitted("java.lang.Integer compareTo(java.lang.Object)", compareTo);
        assertPermitted("java.lang.Integer compareTo(java.lang.Integer)", compareTo);
        assertPermitted("java.util.List add(java.lang.Object)", ArrayList.class.getMethod("add", Object.class));
        // a bridge method runs the same code as a virtual call to the method it bridges, so it matches through it
        final Method bridge = Integer.class.getMethod("compareTo", Object.class);
        Assert.assertTrue(bridge.isBridge());
        assertPermitted("java.lang.Comparable compareTo(java.lang.Object)", bridge);
        assertPermitted("java.lang.Integer compareTo(java.lang.Object)", bridge);
        assertNotPermitted("java.lang.Integer compareTo(java.lang.Integer)", bridge);
        // including a bridge for an override that narrows a generic return type
        final Method narrowingBridge = UpperCase.class.getMethod("apply", Object.class);
        Assert.assertTrue(narrowingBridge.isBridge());
        Assert.assertEquals(Object.class, narrowingBridge.getReturnType());
        final String transform = Transform.class.getName();
        assertPermitted(transform + " apply(..)", narrowingBridge);
        assertPermitted(transform + " apply(java.lang.Object)", narrowingBridge);
        assertPermitted(transform + " apply(..)", UpperCase.class.getMethod("apply", String.class));
        assertNotPermitted(transform + " apply(..)", UpperCase.class.getMethod("apply", Integer.class));
    }

    @Test
    public void testConstructors() throws NoSuchMethodException {
        assertPermitted("java.lang.Integer <constructor>(int)", Integer.class.getConstructor(int.class));
        assertNotPermitted("java.lang.Integer <constructor>(int)", Integer.class.getConstructor(String.class));
        assertPermitted("java.lang.Integer <constructor>(..)", Integer.class.getConstructor(String.class));
        assertPermitted("java.math.BigInteger <constructor>(String)", BigInteger.class.getConstructor(String.class));
        assertNotPermitted("java.math.BigInteger <constructor>(String)", BigDecimal.class.getConstructor(String.class));
        assertNotPermitted("java.lang.Number <constructor>()", Integer.class.getConstructor(int.class));
        // a method name of only wildcards matches constructors too
        assertPermitted("java.math.BigDecimal *(..)", BigDecimal.class.getConstructor(String.class));
        assertPermitted("java.lang.String *(char[])", String.class.getConstructor(char[].class));
        assertPermitted("*..* *(..)", Integer.class.getConstructor(int.class));
        assertNotPermitted("java.lang.String value*(..)", String.class.getConstructor(char[].class));
        assertNotPermitted("java.lang.String <constructor>(..)", String.class.getMethod("valueOf", char[].class));
    }

    @Test
    public void testClassWithoutCanonicalName() throws NoSuchMethodException {
        final Class<?> local = localClass();
        Assert.assertNull(local.getCanonicalName());
        final Method value = local.getMethod("value");
        assertPermitted(local.getName() + " value()", value);
        assertPermitted(getClass().getName() + "$*Local value()", value);
        assertNotPermitted(getClass().getName() + "$*Other value()", value);
        assertPermitted("java.lang.Object toString()", local.getMethod("toString"));

        // nor do arrays of a local class have a canonical name
        final Class<?> localArray = local.arrayType();
        Assert.assertNull(localArray.getCanonicalName());
        final Method accept = local.getMethod("accept", localArray);
        assertPermitted("*..* accept(*)", accept);
        assertPermitted("*..* accept(*[])", accept);
        assertPermitted("*..* accept(" + local.getName() + "[])", accept);
        assertNotPermitted("*..* accept(" + local.getName() + ")", accept);
        assertNotPermitted("*..* accept(java.lang.Object[])", accept);
        final Constructor<?> constructor = local.getConstructor(localArray);
        assertPermitted("*..* <constructor>(*)", constructor);
        assertPermitted("*..* <constructor>(" + local.getName() + "[])", constructor);
        assertNotPermitted("*..* <constructor>(" + local.getName() + ")", constructor);
    }

    /**
     * A local class declared in a static method, so that its constructors take no enclosing instance.
     */
    private static Class<?> localClass() {
        class Local {
            public Local(final Local[] values) {}

            public int value() {
                return 1;
            }

            public void accept(final Local[] values) {}
        }
        return Local.class;
    }

    @Test
    public void testClassFromAnotherClassLoader() throws NoSuchMethodException {
        // a proxy class lives in the class loader that defines it, so it cannot be found by name elsewhere
        final ClassLoader child = new URLClassLoader(new URL[0], getClass().getClassLoader());
        final Class<?> proxyClass = Proxy.newProxyInstance(child, new Class<?>[] {CharSequence.class},
                (proxy, method, args) -> null).getClass();
        Assert.assertEquals(child, proxyClass.getClassLoader());

        assertPermitted("java.lang.CharSequence length()", proxyClass.getMethod("length"));
        assertPermitted("java.lang.Object toString()", proxyClass.getMethod("toString"));
        assertNotPermitted("java.lang.CharSequence length()", proxyClass.getMethod("hashCode"));
        assertNotPermitted("java.lang.String length()", proxyClass.getMethod("length"));
    }

    @Test
    public void testPackagePrivateOverrides() throws Exception {
        final String pattern = PackagePrivateBase.class.getName() + " packagePrivate()";
        assertPermitted(pattern, PackagePrivateSub.class.getDeclaredMethod("packagePrivate"));
        assertPermitted(pattern, PackagePrivatePublicSub.class.getDeclaredMethod("packagePrivate"));

        // a method of the same name in another package does not override a package-private method
        assertNotPermitted(pattern, OtherPackageSub.class.getDeclaredMethod("packagePrivate"));

        // an override in another package of a public override in the base's package overrides the base too
        assertPermitted(pattern, OtherPackageTransitiveSub.class.getDeclaredMethod("packagePrivate"));
        // but not when the override in the base's package is itself package-private
        assertNotPermitted(pattern, OtherPackageNonWideningSub.class.getDeclaredMethod("packagePrivate"));
        assertNotPermitted(PackagePrivateSub.class.getName() + " packagePrivate()",
                OtherPackageNonWideningSub.class.getDeclaredMethod("packagePrivate"));

        // the same package name in another class loader is a different runtime package, so there is no override
        for (final Class<?> sub : List.of(PackagePrivateSub.class, PackagePrivatePublicSub.class)) {
            final Class<?> childSub = loadInChildLoader(sub);
            Assert.assertNotEquals(sub, childSub);
            Assert.assertEquals(PackagePrivateBase.class, childSub.getSuperclass());
            assertNotPermitted(pattern, childSub.getDeclaredMethod("packagePrivate"));
        }
    }

    @Test
    public void testGenericErasure() throws NoSuchMethodException {
        final String base = GenericBase.class.getName();

        // a type variable bound by the implementing class, beside a parameterized parameter type
        final Method integerTypeVariable =
                IntegerImpl.class.getMethod("typeVariable", Integer.class, List.class);
        assertPermitted(base + " typeVariable(..)", integerTypeVariable);
        assertPermitted(base + " typeVariable(java.lang.Object, java.util.List)", integerTypeVariable);
        assertPermitted(base + " typeVariable(java.lang.Integer, java.util.List)", integerTypeVariable);
        // an overload whose parameter does not erase to the bound type argument does not override
        assertNotPermitted(base + " typeVariable(..)",
                IntegerImpl.class.getMethod("typeVariable", String.class, List.class));

        // a generic array parameter
        final Method integerArray = IntegerImpl.class.getMethod("array", Integer[].class);
        assertPermitted(base + " array(java.lang.Object[])", integerArray);
        assertPermitted(base + " array(java.lang.Integer[])", integerArray);
        assertNotPermitted(base + " array(..)", IntegerImpl.class.getMethod("array", String[].class));

        // a method type variable, which no class binds, erases to its bound
        final Method integerMethodVariable =
                IntegerImpl.class.getMethod("methodVariable", Integer.class, Number.class);
        assertPermitted(base + " methodVariable(java.lang.Object, java.lang.Number)", integerMethodVariable);
        assertPermitted(base + " methodVariable(java.lang.Integer, java.lang.Number)", integerMethodVariable);

        // a type argument that is itself parameterized
        assertPermitted(base + " typeVariable(..)", ListImpl.class.getMethod("typeVariable", List.class, List.class));
        assertPermitted(base + " array(java.lang.Object[])", ListImpl.class.getMethod("array", List[].class));

        // a type variable bound to the type variable of an intermediate class, which the subclass binds
        final Method leafTypeVariable = Leaf.class.getMethod("typeVariable", Long.class, List.class);
        assertPermitted(base + " typeVariable(java.lang.Object, java.util.List)", leafTypeVariable);
        assertPermitted(base + " array(java.lang.Object[])", Leaf.class.getMethod("array", Long[].class));
        assertNotPermitted(base + " typeVariable(..)", Leaf.class.getMethod("typeVariable", Integer.class, List.class));

        // a type variable of the generic class that encloses an inner class, bound by the subclass's supertype
        final String inner = Outer.Inner.class.getName();
        final Method accept = InnerSub.class.getMethod("accept", String.class);
        assertPermitted(inner + " accept(..)", accept);
        assertPermitted(inner + " accept(java.lang.Object)", accept);
        assertNotPermitted(inner + " accept(..)", InnerSub.class.getMethod("accept", Integer.class));

        // an inner class's superclass binds the type variables of its supertypes, whatever the owner type binds
        final String shadow = Shadow.class.getName();
        assertPermitted(shadow + " accept(..)", ShadowLeaf.class.getMethod("accept", String.class));
        assertNotPermitted(shadow + " accept(..)", ShadowLeaf.class.getMethod("accept", Integer.class));

        // the supertypes of a raw type are erased, so a type variable erases to the bound in its own declaration
        final String sink = Sink.class.getName();
        assertPermitted(sink + " accept(..)", RawSinkSub.class.getMethod("accept", Object.class));
        assertNotPermitted(sink + " accept(..)", RawSinkSub.class.getMethod("accept", Number.class));
        final String root = Root.class.getName();
        assertPermitted(root + " accept(..)", RawRootSub.class.getMethod("accept", Object.class));
        assertNotPermitted(root + " accept(..)", RawRootSub.class.getMethod("accept", Number.class));
        // an inner class of a generic class, referred to without type arguments, is raw too
        assertPermitted(sink + " accept(..)", RawSinkInnerSub.class.getMethod("accept", Object.class));
        assertNotPermitted(sink + " accept(..)", RawSinkInnerSub.class.getMethod("accept", Number.class));
        assertNotPermitted(root + " accept(java.lang.Object)", RawRootInnerSub.class.getMethod("accept", String.class));
        assertPermitted(root + " accept(java.lang.Object)", LongRootInnerSub.class.getMethod("accept", String.class));
        // but a static nested class of a generic class is not
        assertPermitted(root + " accept(java.lang.Object)",
                StaticNestedSub.class.getMethod("accept", String.class));
        // the type parameters of the class around a local class do not make the local class raw
        final Class<?> localRootSub = GenericOuter.localRootSub();
        assertPermitted(root + " accept(java.lang.Object)", localRootSub.getMethod("accept", String.class));
        // while a parameterized supertype binds them
        assertPermitted(root + " accept(..)", LongRootSub.class.getMethod("accept", Long.class));
        assertNotPermitted(root + " accept(..)", LongRootSub.class.getMethod("accept", Number.class));
    }

    @Test
    public void testCandidatesThatCannotBeOverridden() throws NoSuchMethodException {
        final String base = CandidateBase.class.getName();
        // a private method in the supertype is not overridden, whatever the subclass's method's access
        assertNotPermitted(base + " hidden()", CandidateSub.class.getMethod("hidden"));
        // a private method is not matched through its supertypes either
        assertNotPermitted(base + " secret()", CandidateSub.class.getDeclaredMethod("secret"));
        assertPermitted(CandidateSub.class.getName() + " secret()", CandidateSub.class.getDeclaredMethod("secret"));
        // a static method in the supertype is not overridden
        assertNotPermitted(base + " shared(..)", CandidateSub.class.getMethod("shared", long.class));
        // a method with the same name and a different number of parameters does not override
        assertNotPermitted(base + " count(..)", CandidateSub.class.getMethod("count"));
        assertPermitted(base + " count(..)", CandidateSub.class.getMethod("count", int.class));

        // the supertype's compiler-generated bridge method does not make an override match a pattern for it
        final String bridgeBase = BridgeBase.class.getName();
        final Method compareTo = BridgeSub.class.getMethod("compareTo", BridgeBase.class);
        assertNotPermitted(bridgeBase + " compareTo(java.lang.Object)", compareTo);
        assertPermitted(bridgeBase + " compareTo(" + bridgeBase + ")", compareTo);
    }

    @Test
    public void testProtectedOverrides() throws NoSuchMethodException {
        final String pattern = ProtectedBase.class.getName() + " prot()";
        // unlike a package-private method, a protected method is overridden from another package
        assertPermitted(pattern, OtherPackageProtectedSub.class.getMethod("prot"));
        assertPermitted(pattern, OtherPackageProtectedSub.class.getDeclaredMethod("prot"));
        assertNotPermitted(pattern, OtherPackageProtectedSub.class.getMethod("unrelated"));
    }

    private Class<?> loadInChildLoader(final Class<?> type) throws ClassNotFoundException {
        return new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
                if (!name.equals(type.getName())) {
                    return super.loadClass(name, resolve);
                }
                final String resource = name.replace('.', '/') + ".class";
                try (final java.io.InputStream in = getParent().getResourceAsStream(resource)) {
                    final byte[] bytes = in.readAllBytes();
                    return defineClass(name, bytes, 0, bytes.length);
                } catch (java.io.IOException e) {
                    throw new ClassNotFoundException(name, e);
                }
            }
        }.loadClass(type.getName());
    }

    @Test
    public void testSeparatorsAndWhitespace() throws NoSuchMethodException {
        final Method length = String.class.getMethod("length");
        assertPermitted("java.lang.String#length()", length);
        assertPermitted("java.lang.String # length()", length);
        assertPermitted("  java.lang.String   length ( )  ", length);
        final Method substring = String.class.getMethod("substring", int.class, int.class);
        assertPermitted("java.lang.String substring(int,int)", substring);
        assertPermitted("java.lang.String substring( int , int )", substring);
        assertPermitted("java.lang.String#substring(..,int)", substring);
    }

    @Test
    public void testInvalidPatterns() {
        for (final String invalid : List.of("", "java.lang.String", "java.lang.String length", "length()",
                "java.lang.String length(", "java.lang.String len-gth()", "java.lang.String length(int,)",
                "java.lang.String length(java.lang.Object..., int)", "* toString()",
                // malformed type patterns, in the declaring class and in the argument list
                ".java.lang.String length()", "java.lang.String. length()", "java...String length()",
                "java.lang.Str-ing length()", "java.lang.String valueOf(java.lang.Ob#ject)",
                "java.lang.String valueOf(java..lang...Object)", "java.lang.String valueOf(.Object)",
                "java.lang.String valueOf([])",
                // at most one ".." in an argument list, and names do not start with a digit
                "java.lang.String *(.., int, ..)", "java.lang.String 1length()", "java.1lang.String length()",
                "java.lang.String # # length()", "java.lang.String length() trailing",
                // only spaces separate the elements of a pattern
                "java.lang.String\tlength()", "java.lang.String\t#length()", "java.lang.String substring(int,\tint)")) {
            Assert.assertThrows(invalid, UncheckedDeephavenException.class,
                    () -> new MethodListInvocationValidator(List.of(invalid)));
        }
    }

    private static void assertPermitted(final String pattern, final Executable executable) {
        Assert.assertEquals(pattern + " should match " + executable, Boolean.TRUE, permit(pattern, executable));
    }

    private static void assertNotPermitted(final String pattern, final Executable executable) {
        Assert.assertNull(pattern + " should not match " + executable, permit(pattern, executable));
    }

    private static Boolean permit(final String pattern, final Executable executable) {
        final MethodListInvocationValidator validator = new MethodListInvocationValidator(List.of(pattern));
        if (executable instanceof Method) {
            return validator.permitMethod((Method) executable);
        }
        return validator.permitConstructor((Constructor<?>) executable);
    }

    public interface GenericBase<T> {
        void typeVariable(T value, List<String> list);

        void array(T[] values);

        <U extends Number> void methodVariable(T value, U number);
    }

    public static class IntegerImpl implements GenericBase<Integer> {
        @Override
        public void typeVariable(final Integer value, final List<String> list) {}

        public void typeVariable(final String value, final List<String> list) {}

        @Override
        public void array(final Integer[] values) {}

        public void array(final String[] values) {}

        @Override
        public <U extends Number> void methodVariable(final Integer value, final U number) {}
    }

    public static class ListImpl implements GenericBase<List<Integer>> {
        @Override
        public void typeVariable(final List<Integer> value, final List<String> list) {}

        @Override
        public void array(final List<Integer>[] values) {}

        @Override
        public <U extends Number> void methodVariable(final List<Integer> value, final U number) {}
    }

    public abstract static class Middle<E extends Number> implements GenericBase<E> {
    }

    public static class Leaf extends Middle<Long> {
        @Override
        public void typeVariable(final Long value, final List<String> list) {}

        public void typeVariable(final Integer value, final List<String> list) {}

        @Override
        public void array(final Long[] values) {}

        @Override
        public <U extends Number> void methodVariable(final Long value, final U number) {}
    }

    public static class CandidateBase {
        private int hidden() {
            return 1;
        }

        private int secret() {
            return 1;
        }

        public static int shared(final int value) {
            return value;
        }

        public int count(final int value) {
            return value;
        }
    }

    public static class CandidateSub extends CandidateBase {
        public int hidden() {
            return 2;
        }

        private int secret() {
            return 2;
        }

        public int shared(final long value) {
            return 2;
        }

        public int count() {
            return 2;
        }

        @Override
        public int count(final int value) {
            return 2;
        }
    }

    public static class BridgeBase implements Comparable<BridgeBase> {
        @Override
        public int compareTo(final BridgeBase other) {
            return 0;
        }
    }

    public static class BridgeSub extends BridgeBase {
        @Override
        public int compareTo(final BridgeBase other) {
            return 1;
        }
    }

    public static class Outer<T> {
        public class Inner {
            public void accept(final T value) {}
        }
    }

    public static class InnerSub extends Outer<String>.Inner {
        public InnerSub(final Outer<String> outer) {
            outer.super();
        }

        @Override
        public void accept(final String value) {}

        public void accept(final Integer value) {}
    }

    public static class Shadow<T> {
        public void accept(final T value) {}

        public class Inner extends Shadow<String> {
        }
    }

    public static class ShadowLeaf extends Shadow<Integer>.Inner {
        public ShadowLeaf(final Shadow<Integer> outer) {
            outer.super();
        }

        @Override
        public void accept(final String value) {}

        public void accept(final Integer value) {}
    }

    public interface Sink<T> {
        void accept(T value);
    }

    public abstract static class NumberSink<T extends Number> implements Sink<T> {
    }

    @SuppressWarnings("rawtypes")
    public static class RawSinkSub extends NumberSink {
        @Override
        public void accept(final Object value) {}

        public void accept(final Number value) {}
    }

    public static class Root<T> {
        public void accept(final T value) {}
    }

    public static class NumberRoot<S extends Number> extends Root<S> {
    }

    @SuppressWarnings("rawtypes")
    public static class RawRootSub extends NumberRoot {
        @Override
        public void accept(final Object value) {}

        public void accept(final Number value) {}
    }

    public static class LongRootSub extends NumberRoot<Long> {
        @Override
        public void accept(final Long value) {}

        public void accept(final Number value) {}
    }

    public static class GenericOuter<T extends Number> {
        public abstract class SinkInner implements Sink<T> {
        }

        public class RootInner extends Root<String> {
        }

        public static class StaticNested extends Root<String> {
        }

        /**
         * A local subclass of a local class, both declared in a static method of this generic class.
         */
        static Class<?> localRootSub() {
            class LocalRoot extends Root<String> {
            }
            class LocalRootSub extends LocalRoot {
                @Override
                public void accept(final String value) {}
            }
            return LocalRootSub.class;
        }
    }

    public interface CloneableInterface {
        Object clone();
    }

    @SuppressWarnings("rawtypes")
    public static class RawSinkInnerSub extends GenericOuter.SinkInner {
        public RawSinkInnerSub(final GenericOuter outer) {
            outer.super();
        }

        @Override
        public void accept(final Object value) {}

        public void accept(final Number value) {}
    }

    @SuppressWarnings("rawtypes")
    public static class RawRootInnerSub extends GenericOuter.RootInner {
        public RawRootInnerSub(final GenericOuter outer) {
            outer.super();
        }

        public void accept(final String value) {}
    }

    public static class LongRootInnerSub extends GenericOuter<Long>.RootInner {
        public LongRootInnerSub(final GenericOuter<Long> outer) {
            outer.super();
        }

        @Override
        public void accept(final String value) {}
    }

    public static class StaticNestedSub extends GenericOuter.StaticNested {
        @Override
        public void accept(final String value) {}
    }

    public interface Transform<T> {
        T apply(T value);
    }

    public static class UpperCase implements Transform<String> {
        @Override
        public String apply(final String value) {
            return value.toUpperCase();
        }

        public Integer apply(final Integer value) {
            return value;
        }
    }

    public static class CustomBigInteger extends BigInteger {
        public CustomBigInteger() {
            super("1");
        }

        @Override
        public BigInteger add(final BigInteger value) {
            return value;
        }

        @Override
        public String toString() {
            return "custom";
        }

        public int extra() {
            return 1;
        }
    }

    public static class CustomBigDecimal extends BigDecimal {
        public CustomBigDecimal() {
            super(1);
        }

        @Override
        public int scale() {
            return 0;
        }

        public int extra() {
            return 1;
        }
    }

    public static class ArrayParameters {
        public static void vector(final String[] values) {}

        public static void matrix(final String[][] values) {}

        public static void primitiveMatrix(final int[][] values) {}
    }

    public static class ProtectedBase {
        protected int prot() {
            return 1;
        }
    }
}
