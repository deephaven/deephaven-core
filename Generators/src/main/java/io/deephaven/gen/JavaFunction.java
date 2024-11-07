//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.gen;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

/**
 * A Java function description for use in code generation.
 * <p>
 * JavaFunctions are equal if they have the same method names and parameter types.
 */
public class JavaFunction implements Comparable<JavaFunction> {
    private static final Logger log = Logger.getLogger(JavaFunction.class.toString());

    private final String className;
    private final String classNameShort;
    private final String methodName;
    private final TypeVariable<Method>[] typeParameters;
    private final Type returnType;
    private final Type[] parameterTypes;
    private final String[] parameterNames;
    private final boolean isVarArgs;

    public JavaFunction(final String className, final String classNameShort, final String methodName,
            final TypeVariable<Method>[] typeParameters, final Type returnType, final Type[] parameterTypes,
            final String[] parameterNames, final boolean isVarArgs) {
        this.className = className;
        this.classNameShort = classNameShort;
        this.methodName = methodName;
        this.typeParameters = typeParameters;
        this.returnType = returnType;
        this.parameterTypes = parameterTypes;
        this.parameterNames = parameterNames;
        this.isVarArgs = isVarArgs;
    }

    public JavaFunction(final Method m) {
        this(
                m.getDeclaringClass().getCanonicalName(),
                m.getDeclaringClass().getSimpleName(),
                m.getName(),
                m.getTypeParameters(),
                m.getGenericReturnType(),
                m.getGenericParameterTypes(),
                Arrays.stream(m.getParameters()).map(Parameter::getName).toArray(String[]::new),
                m.isVarArgs());

        for (Parameter parameter : m.getParameters()) {
            if (!parameter.isNamePresent()) {
                throw new IllegalArgumentException(
                        "Parameter names are not present in the code!  Was the code compiled with \"-parameters\": "
                                + this);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        JavaFunction that = (JavaFunction) o;

        if (!Objects.equals(methodName, that.methodName))
            return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(parameterTypes, that.parameterTypes);

    }

    @Override
    public int hashCode() {
        int result = methodName != null ? methodName.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(parameterTypes);
        return result;
    }

    @Override
    public String toString() {
        return "JavaFunction{" +
                "className='" + className + '\'' +
                ", methodName='" + methodName + '\'' +
                ", typeParameters=" + Arrays.toString(typeParameters) +
                ", returnType=" + returnType +
                ", parameterTypes=" + Arrays.toString(parameterTypes) +
                ", parameterNames=" + Arrays.toString(parameterNames) +
                '}';
    }

    @Override
    public int compareTo(@NotNull JavaFunction o) {
        int cm = methodName.compareTo(o.methodName);
        if (cm != 0) {
            return cm;
        }
        if (parameterTypes.length != o.parameterTypes.length) {
            return parameterTypes.length - o.parameterTypes.length;
        }

        for (int i = 0; i < parameterTypes.length; i++) {
            Type t1 = parameterTypes[i];
            Type t2 = o.parameterTypes[i];
            int ct = t1.toString().compareTo(t2.toString());
            if (ct != 0) {
                return ct;
            }
        }

        return 0;
    }

    public String getClassName() {
        return className;
    }

    public String getClassNameShort() {
        return classNameShort;
    }

    public String getMethodName() {
        return methodName;
    }

    public TypeVariable<Method>[] getTypeParameters() {
        return typeParameters;
    }

    public Type getReturnType() {
        return returnType;
    }

    public Class<?> getReturnClass() {
        if (returnType == null) {
            return null;
        }

        try {
            return getErasedType(returnType);
        } catch (UnsupportedOperationException e) {
            log.warning("Unable to determine Class from returnType=" + returnType.getTypeName());
            return null;
        }
    }

    /**
     * Determine the Class from the Type.
     */
    private static Class<?> getErasedType(Type paramType) {
        if (paramType instanceof Class) {
            return (Class<?>) paramType;
        } else if (paramType instanceof ParameterizedType) {
            return (Class<?>) // We are asking the parameterized type for its raw type, which is always Class
            ((ParameterizedType) paramType).getRawType();
        } else if (paramType instanceof WildcardType) {
            final Type[] upper = ((WildcardType) paramType).getUpperBounds();
            return getErasedType(upper[0]);
        } else if (paramType instanceof java.lang.reflect.TypeVariable) {
            final Type[] bounds = ((TypeVariable<?>) paramType).getBounds();
            if (bounds.length > 1) {
                Class<?>[] erasedBounds = new Class[bounds.length];
                Class<?> weakest = null;
                for (int i = 0; i < erasedBounds.length; i++) {
                    erasedBounds[i] = getErasedType(bounds[i]);
                    if (i == 0) {
                        weakest = erasedBounds[i];
                    } else {
                        weakest = getWeakest(weakest, erasedBounds[i]);
                    }
                    // If we are erased to object, stop erasing...
                    if (weakest == Object.class) {
                        break;
                    }
                }
                return weakest;
            }
            return getErasedType(bounds[0]);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Determine the weakest parent of the two provided Classes.
     *
     * @param one one class to compare
     * @param two the other class to compare
     * @return the weakest parent Class
     */
    private static Class<?> getWeakest(Class<?> one, Class<?> two) {
        if (one.isAssignableFrom(two)) {
            return one;
        } else if (two.isAssignableFrom(one)) {
            return two;
        }
        // No luck on quick check... Look in interfaces.
        Set<Class<?>> oneInterfaces = getFlattenedInterfaces(one);
        Set<Class<?>> twoInterfaces = getFlattenedInterfaces(two);
        // Keep only shared interfaces
        oneInterfaces.retainAll(twoInterfaces);
        Class<?> strongest = Object.class;
        for (Class<?> cls : oneInterfaces) {
            // There is a winning type...
            if (strongest.isAssignableFrom(cls)) {
                strongest = cls;
            } else if (!cls.isAssignableFrom(strongest)) {
                return Object.class;
            }
        }
        // Will be Object.class if there were no shared interfaces (or shared interfaces were not compatible).
        return strongest;
    }

    private static Set<Class<?>> getFlattenedInterfaces(Class<?> cls) {
        final Set<Class<?>> set = new HashSet<>();
        while (cls != null && cls != Object.class) {
            for (Class<?> iface : cls.getInterfaces()) {
                collectInterfaces(set, iface);
            }
            cls = cls.getSuperclass();
        }
        return set;
    }

    private static void collectInterfaces(final Collection<Class<?>> into, final Class<?> cls) {
        if (into.add(cls)) {
            for (final Class<?> iface : cls.getInterfaces()) {
                if (into.add(iface)) {
                    collectInterfaces(into, iface);
                }
            }
        }
    }

    public Type[] getParameterTypes() {
        return parameterTypes;
    }

    public String[] getParameterNames() {
        return parameterNames;
    }

    public boolean isVarArgs() {
        return isVarArgs;
    }

    /**
     * Creates a new JavaFunction with the same signature, but with new class and method names.
     *
     * @param className class name or null if the current name should be used.
     * @param classNameShort short class name or null if the current short name should be used.
     * @param methodName method name or null if the current name should be used.
     * @param returnType return type or null if the current return type should be used.
     * @return a new JavaFunction with the same signature, but with new class and method names.
     */
    public JavaFunction transform(final String className, final String classNameShort, final String methodName,
            Type returnType) {
        return new JavaFunction(
                className == null ? this.className : className,
                classNameShort == null ? this.classNameShort : classNameShort,
                methodName == null ? this.methodName : methodName,
                getTypeParameters(),
                returnType == null ? getReturnType() : returnType,
                getParameterTypes(),
                getParameterNames(),
                isVarArgs());
    }
}
