//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.gen;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.*;
import java.util.*;
import java.util.logging.Logger;

@SuppressWarnings("StringConcatenationInLoop")
public class GenUtils {
    private GenUtils() {}

    private static final Logger log = Logger.getLogger(GenUtils.class.toString());

    private static final TIntObjectMap<String> cachedIndents = new TIntObjectHashMap<>();

    /**
     * Get a String of spaces for indenting code.
     *
     * @param n The number of indents
     * @return The String for indenting code with spaces
     */
    public static String indent(final int n) {
        String cached = cachedIndents.get(n);
        if (cached == null) {
            cachedIndents.put(n, cached = String.join("", Collections.nCopies(4 * n, " ")));
        }
        return cached;
    }

    /**
     * Creates the header for a generated java file.
     *
     * @param generatorClass class used to generate the code.
     * @param gradleTask gradle task to generate the code.
     * @return The header for a generated java file
     */
    public static String javaHeader(final Class<?> generatorClass, final String gradleTask) {
        return String.join("\n",
                "//",
                "// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending",
                "//",
                "// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY",
                "// ****** Run " + generatorClass.getSimpleName() + " or \"./gradlew " + gradleTask
                        + "\" to regenerate",
                "//",
                "// @formatter:off",
                "");
    }

    /**
     * Assert that the generated code is the same as the old code.
     *
     * @param generatorClass class used to generate the code.
     * @param gradleTask gradle task to generate the code.
     * @param oldCode old code
     * @param newCode new code
     */
    public static void assertGeneratedCodeSame(final Class<?> generatorClass, final String gradleTask,
            final String oldCode, final String newCode) {
        if (!newCode.equals(oldCode)) {
            throw new RuntimeException(
                    "Change in generated code.  Run " + generatorClass + " or \"./gradlew " + gradleTask
                            + "\" to regenerate\n");
        }
    }

    /**
     * Generates a set of Java imports needed for the given type.
     *
     * @param t type.
     * @return set of imports.
     */
    public static Set<String> typesToImport(Type t) {
        Set<String> result = new LinkedHashSet<>();

        if (t instanceof Class) {
            final Class<?> c = (Class<?>) t;
            final boolean isArray = c.isArray();
            final boolean isPrimitive = c.isPrimitive();

            if (isPrimitive) {
                return result;
            } else if (isArray) {
                return typesToImport(c.getComponentType());
            } else {
                result.add(t.getTypeName());
            }
        } else if (t instanceof ParameterizedType) {
            final ParameterizedType pt = (ParameterizedType) t;
            result.add(pt.getRawType().getTypeName());

            for (Type a : pt.getActualTypeArguments()) {
                result.addAll(typesToImport(a));
            }
        } else if (t instanceof TypeVariable) {
            // type variables are generic so they don't need importing
            return result;
        } else if (t instanceof WildcardType) {
            // type variables are generic so they don't need importing
            return result;
        } else if (t instanceof GenericArrayType) {
            GenericArrayType at = (GenericArrayType) t;
            return typesToImport(at.getGenericComponentType());
        } else {
            throw new UnsupportedOperationException("Unsupported Type type: " + t.getClass());
        }

        return result;
    }

    /**
     * Generates a set of Java imports needed for the given functions.
     *
     * @param functions functions.
     * @return set of imports.
     */
    public static Set<String> typesToImport(final Set<JavaFunction> functions) {
        Set<String> imports = new TreeSet<>();

        for (JavaFunction f : functions) {
            imports.add(f.getClassName());

            imports.addAll(typesToImport(f.getReturnType()));

            for (Type t : f.getParameterTypes()) {
                imports.addAll(typesToImport(t));
            }
        }

        return imports;
    }

    /**
     * Helper to transform method parameter types to a form that can be used in a javadoc link, including removing
     * generics and finding the upper bound of typevars.
     */
    @NotNull
    public static String javadocLinkParamTypeString(Type t) {
        if (t instanceof ParameterizedType) {
            return ((ParameterizedType) t).getRawType().getTypeName();
        } else if (t instanceof TypeVariable) {
            return javadocLinkParamTypeString(((TypeVariable<?>) t).getBounds()[0]);
        } else if (t instanceof WildcardType) {
            return javadocLinkParamTypeString(((WildcardType) t).getUpperBounds()[0]);
        } else if (t instanceof GenericArrayType) {
            return javadocLinkParamTypeString(((GenericArrayType) t).getGenericComponentType()) + "[]";
        }
        return t.getTypeName();
    }

    /**
     * Creates an argument string for a Java function.
     *
     * @param f function.
     * @param includeTypes true to include types of the argument parameters; false to just include the parameter names.
     * @return argument string.
     */
    public static String javaArgString(final JavaFunction f, boolean includeTypes) {
        String callArgs = "";

        for (int i = 0; i < f.getParameterTypes().length; i++) {
            if (i != 0) {
                callArgs += ",";
            }

            Type t = f.getParameterTypes()[i];

            String typeString = t.getTypeName().replace("$", ".");

            if (f.isVarArgs() && i == f.getParameterTypes().length - 1) {
                final int index = typeString.lastIndexOf("[]");
                typeString = typeString.substring(0, index) + "..." + typeString.substring(index + 2);
            }

            if (includeTypes) {
                callArgs += " " + typeString;
            }

            callArgs += " " + f.getParameterNames()[i];
        }

        return callArgs;
    }

    /**
     * Create code for a Java function signature.
     *
     * @param f function signature.
     * @param sigPrefix sigPrefix to add to the function signature (e.g. "public static").
     * @return code for the function signature.
     */
    public static String javaFunctionSignature(final JavaFunction f, final String sigPrefix) {
        String returnType = f.getReturnType().getTypeName().replace("$", ".");
        String s = indent(1) + (sigPrefix == null || sigPrefix.equals("") ? "" : (sigPrefix + " "));

        if (f.getTypeParameters().length > 0) {
            s += "<";

            for (int i = 0; i < f.getTypeParameters().length; i++) {
                if (i != 0) {
                    s += ",";
                }

                TypeVariable<Method> t = f.getTypeParameters()[i];
                log.fine("BOUNDS: " + Arrays.toString(t.getBounds()));
                s += t;

                Type[] bounds = t.getBounds();

                if (bounds.length != 1) {
                    throw new RuntimeException("Unsupported bounds: " + Arrays.toString(bounds));
                }

                Type bound = bounds[0];

                if (!bound.equals(Object.class)) {
                    s += " extends " + bound.getTypeName();
                }

            }

            s += ">";
        }

        s += " " + returnType + " " + f.getMethodName() + "(" + javaArgString(f, true) + " )";
        return s;
    }

    /**
     * Create a Java function with the given javadoc and body.
     *
     * @param f function signature.
     * @param sigPrefix sigPrefix to add to the function signature (e.g. "public static").
     * @param javadoc javadoc.
     * @param funcBody function body.
     * @return code for the function.
     */
    public static String javaFunction(final JavaFunction f, final String sigPrefix, final String javadoc,
            final String funcBody) {
        String s = javadoc == null || javadoc.equals("") ? "" : (javadoc + "\n");
        s += javaFunctionSignature(f, sigPrefix);
        s += funcBody;
        return s;
    }
}
